""" This job is for downloading S2 with optional atmospheric correction

Inputs: tile name
Optional Inputs: atmospheric correction method
Outputs: downloaded product uploaded
        to S3 bucket repository for l1 images, atmospherically corrected product uploaded to S3 bucket for corrected imagery

Dependencies: sen2cor, maccs-maja

Major Steps

1. Download the tile from USGS if possible use (s2d2 or landsat_downloader or sentinel_downloader), check if the tile exists in S3 L1 product repo, check if atmospheric correction result is in appropriate S3 bucket

2. Run correction with sen2cor or MACCS-MAJA if specified

3. Upload results to S3 bucket(s)

4. Remove local working copies

5. Report job results to job_manager API

Notes:
In the future, a convertion to a COG tif (cloud optimized geotiff) would be a
good addition.
"""

# system modules
import datetime
import json
import multiprocessing
import os
from pathlib import Path
import re
import shutil
from subprocess import Popen, PIPE
import sys
import tarfile
import zipfile

from functools import partial

# 3rd party modules
import boto3
from botocore.client import Config
import botocore
import click
from osgeo import gdal
import redis


from celery import shared_task, task, app, states

import time

import logging

# 2nd party modules
from landsat_downloader import l8_downloader

# S3 Configuration
MAX_S3_ATTEMPTS = 3

# Internal Minio Config

INPUT_BUCKET_NAME = "l8-l1c-archive"
OUTPUT_BUCKET_NAME = "l8-l2a-products"

# # Create your tasks here
from celery import shared_task, task, app

import logging

# 2nd party modules
from landsat_downloader import l8_downloader
from sentinel_downloader import s2_downloader

import platform

from common.s3_utils import S3Utility, ProgressPercentage
from common.utils import (
    TaskStatus,
    unarchive,
    clean_up_folder,
    run_cmd,
    mp_run,
    get_partial_path,
    TaskFailureException,
)

from django.conf import settings

CONFIG_FILE = "config.yaml"
CONFIG_FILE_PATH = Path(settings.BASE_DIR, CONFIG_FILE)

WORKING_FOLDER_PATH = Path(settings.BASE_DIR, "working_folder")
STATUS_FOLDER_PATH = Path(WORKING_FOLDER_PATH, "status")

s3_helper = S3Utility(settings.S3_CONFIG, WORKING_FOLDER_PATH)

module_logger = logging.getLogger("worker.tasks.download_l8")

redis_instance = redis.StrictRedis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=0,
    password=settings.REDIS_PASS,
    decode_responses=True,
)

def download_using_landsat_downloader(tile_name, entity_id, celery_task=None):

    downloader = l8_downloader.L8Downloader(CONFIG_FILE_PATH, verbose=False)

    # download_product requires a product dict, the minimum product dict has:
    # platform_name ['Landsat-8', 'Sentinel-2']
    # dataset_name ['LANDSAT_8', 'SENTINEL_2A']
    # name [name for the file downloaded to be called locally]
    # entity_id ['L1C_T12UVA_A006488_20180603T183037']
    # product_type = ['FR_BUND' archive, FR_THERM, FR_QB, FR_REFL images, STANDARD tar.gz] for Landsat8
    # product_type = ['FRB' .jpg, STANDARD .zip] for Sentinel 2

    product_dict = {
        "platform_name": "Landsat-8",
        "dataset_name": "LANDSAT_8_C1",
        "entity_id": entity_id,
        "name": tile_name,
    }

    def callback(progress_so_far, total_filesize, percentage_complete):
            celery_task.update_state(
                state=states.STARTED, meta={"download": percentage_complete}
            )

    product_type = "STANDARD"

    result = downloader.download_product(
        product_dict, product_type, directory=WORKING_FOLDER_PATH, callback=callback
    )

    return result


def find_l2a_path(l1c_path):
    module_logger.debug(l1c_path)

    name = Path(l1c_path).name
    l2a_name = None

    module_logger.debug(name)

    if name.startswith('LC08'):
        sat = name.split("_")[0]
        date = name.split("_")[3]
        tile = name.split("_")[2]
        # LC08_L1TP_041024_20190818_20190902_01_T1.tar
        regex_str = r"{}_L1TP_{}_{}_\d{{8}}_01_(RT|T1)".format(
            sat, tile, date
        )
    else:
        sat = name[2]
        date = name.split("_")[2]
        orbit = name.split("_")[4]
        tile = name.split("_")[5]
        regex_str = r"S2{}_MSIL2A_{}_N\d{{4}}_{}_{}_\d{{8}}T\d{{6}}.SAFE".format(
            sat, date, orbit, tile
        )

    # iterate over work dir, find a dir that matches the regex
    for file_name in os.listdir(WORKING_FOLDER_PATH):
        module_logger.debug(file_name)
        search_result = re.search(regex_str, file_name)
        module_logger.debug(search_result)
        if search_result:
            return Path(WORKING_FOLDER_PATH, file_name)

    return l2a_name

def upload_l8_l2a_to_s3(tile_name, celery_task):
    module_logger.info(tile_name)

    name_split = tile_name.split("_")
    path = name_split[2][:3]
    row = name_split[2][3:]

    for file_name in os.listdir(WORKING_FOLDER_PATH):
        # l2a_path = find_l2a_path(extract_result[2])

        # upload_callback = ProgressPercentage(Path(WORKING_FOLDER_PATH), celery_task)

        if file_name.startswith(tile_name):
            upload_success = s3_helper.upload_file_s3(
                Path(WORKING_FOLDER_PATH, file_name),
                str(Path(path, row, tile_name)),
                OUTPUT_BUCKET_NAME,
            )

            if not upload_success:
                return TaskStatus(False, "Upload of L2A Failed", None)

    return TaskStatus(True, "Upload of L2A Succeeded", None)


def check_if_l1c_imagery_exists(imagery_date, l1c_name):

    module_logger.info("checking if imagery exists on s3")
    module_logger.info(l1c_name)

    # search_expression = r"SENTINEL2[A|B]_{}-\d{{6}}-\d{{3}}_L2A_{}_C_V1-0".format(imagery_date, name_parts[1])
    search_expression = l1c_name
    module_logger.info(search_expression)
    module_logger.info("checking if object exists in s3 with wildcards")

    # search_expresssion, object_prefix, bucket_name
    object_exists = s3_helper.check_object_exists_in_s3_wildcards(
        search_expression, "", INPUT_BUCKET_NAME
    )

    return object_exists


def check_if_l2a_imagery_exists(l1c_name):
    module_logger.info("checking if imagery exists on s3")
    module_logger.info(l1c_name)

    l1c_split = l1c_name.split("_")
    path = l1c_split[2][:3]
    row = l1c_split[2][3:]

    # search_expression = r"SENTINEL2[A|B]_{}-\d{{6}}-\d{{3}}_L2A_{}_C_V1-0".format(imagery_date, name_parts[1])
    search_expression = l1c_name
    module_logger.info(search_expression)
    module_logger.info("checking if object exists in s3 with wildcards")

    # search_expresssion, object_prefix, bucket_name
    object_exists = s3_helper.check_object_exists_in_s3_wildcards(
        search_expression, "", OUTPUT_BUCKET_NAME
    )

    return object_exists


@task(bind=True, time_limit=60*60*12, soft_time_limit=60*60*6)
def download_l8_bulk_order(self, order_id):

    downloader = l8_downloader.L8Downloader(CONFIG_FILE_PATH, verbose=False)
    
    redis_instance.set(str(self.request.id) + "_download_progress", 0)
    
    def download_callback(ct, file_name, size, bytes_transferred):
        module_logger.info('Inside download callback')
        module_logger.info(str(ct.request.id))
        current_bytes = int(redis_instance.get(str(ct.request.id) + "_download_progress"))
        current_bytes += bytes_transferred
        percent_complete = float(round((current_bytes / size) * 100, 2))
        module_logger.info(bytes_transferred)
        module_logger.info(percent_complete)

        update_dict = {
        
        }

        module_logger.info(file_name)
        update_dict[file_name] = {
            "status": states.STARTED,
            "download": percent_complete,
            "download_file_size": size
        }

        try:
            ct.update_state(
                task_id=str(ct.request.id),
                state=states.STARTED,
                meta=update_dict
            )
        except BaseException as e:
            module_logger.info('Task isnt ready for updates to meta yet...')
            module_logger.error(str(e))
        
        if percent_complete > 99:
            redis_instance.set(str(ct.request.id) + "_download_progress", 0)
        else:
            redis_instance.set(str(ct.request.id) + "_download_progress", int(current_bytes))


    download_callback_bound = partial(download_callback, self)

    download_folder_path = Path(settings.BASE_DIR, "working_folder")

    try:
        download_result = downloader.download_order(
            order_id, directory=download_folder_path, callback=download_callback_bound
        )
    except BaseException as e:
        module_logger.debug("something went wrong while trying to download")
        download_result = TaskStatus(False, "Download step failed", str(e))

    task_status_list = []
    if download_result.status:

        for file_name in download_result.data:
            
            file_path = Path(WORKING_FOLDER_PATH, file_name)
            # TODO: Extract needs to extract to a folder with the product name instead of to the 
            # root of the working folder
            extract_result = unarchive(file_path, WORKING_FOLDER_PATH)
            if extract_result.status:
                module_logger.debug(extract_result)
                l2a_path = find_l2a_path(extract_result.data)

                l2a_name = extract_result.message

                module_logger.warning(f"L2A Path: {l2a_path}")
                module_logger.info(f"L2A Name: {l2a_name}")

                full_path = l2a_path

                if full_path.is_dir():
                    size = sum(f.stat().st_size for f in full_path.glob('**/*') if f.is_file())
                else:
                    size = float(full_path.stat().st_size)
                
                redis_instance.set(str(self.request.id) + "_upload_progress", 0)

                def upload_callback(ct, file_name, size, bytes_transferred):
                    module_logger.info('Inside upload callback')
                    module_logger.info(str(ct.request.id))
                    current_bytes = int(redis_instance.get(str(ct.request.id) + "_upload_progress"))
                    current_bytes += bytes_transferred
                    percent_complete = float(round((current_bytes / size) * 100, 2))
                    module_logger.info(bytes_transferred)
                    module_logger.info(percent_complete)

                    update_dict = {
                    }

                    module_logger.info(file_name)
                    update_dict[file_name] = {
                        "status": states.STARTED,
                        "upload": percent_complete,
                        "upload_file_size": size
                    }

                    try:
                        ct.update_state(
                            task_id=str(ct.request.id),
                            state=states.STARTED,
                            meta=update_dict
                        )
                    except BaseException as e:
                        module_logger.info('Task isnt ready for updates to meta yet...')
                        module_logger.error(str(e))
                    
                    redis_instance.set(str(ct.request.id) + "_upload_progress", int(current_bytes))

                upload_callback_bound = partial(upload_callback, self, l2a_name, size)

                upload_result = s3_helper.upload_unarchived_product_to_s3_bucket(
                    l2a_path, OUTPUT_BUCKET_NAME, callback=upload_callback_bound
                )

                if upload_result.status:
                # Download, extract and Upload successful, need to upate the individual jobs for each tile
                    task_status_list.append(
                        TaskStatus(True, upload_result.message, None)
                    )
                else:
                    task_status_list.append(
                        TaskStatus(False, f"Upload step failed for {file_name}", None)
                    )
                
            else:
                task_status_list.append(TaskStatus(False, f"Extract step failed for {file_name}", None))
            # Allow check jobs to get task status successfully
            time.sleep(30)
            
    task_status_list.append(download_result)
    
    for task_status in task_status_list:
        if not task_status.status:
            module_logger.error('Failure occurred during bulk download phase')
            module_logger.error(task_status.message)
            raise TaskFailureException(task_status.message)
    
    if upload_result.status:
        module_logger.info("overall job done successfully")

        extract_result = TaskStatus(
            extract_result[0], str(extract_result[1]), str(extract_result[2])
        )
        download_result = TaskStatus(
            download_result[0], str(download_result[1]), str(extract_result[2])
        )

        result_list = [
            ("download", download_result),
            ("extract", extract_result),
            ("upload", upload_result),
        ]

        module_logger.debug(result_list)

    clean_up_folder(WORKING_FOLDER_PATH)
    
    return result_list

def start_job(
    tile_name,
    atmospheric_correction=False,
    ac_res=10,
    api_source="usgs_ee",
    entity_id=None,
    celery_task=None
):
    """Given the name of the tile, start the job processes

    Major Steps:
    1. Check for tile in s2-l1c-archive, download from there
    2. If no tile in s2-l1c-archive, download using landsat_downloader or sentinel_downloader
    3. Unarchive .zip of the product
    4. Run sen2cor or maccs-maja
    5. Upload original l1c .zip to s2-l1c-archive (if it didn't exist in step 1)
    6. Upload atmos cor l2a product to s2-l2a-products (use the tile as the path for organization)
    7. Delete local copies
    8. Report job status.

    """

    # Todo: add checks for existing products (to prevent redundant work)

    download_result = TaskStatus(False, "", "")
    upload_result = TaskStatus(False, "", "")

    module_logger.info(tile_name)

    if not atmospheric_correction:
        l1c_already_exists = check_if_l1c_imagery_exists(None, tile_name)

        if l1c_already_exists[0]:
            module_logger.info("l1c already exists, no need to atmospheric correct")

            return [
                ("download", TaskStatus(True, "l1c already on s3", None), ""),
                ("upload", TaskStatus(True, "l1c already on s3", None), ""),
            ]
        else:
            download_result = download_using_landsat_downloader(tile_name, entity_id, celery_task=celery_task)
            
            if download_result:
                if download_result.status:
                    full_path = Path(WORKING_FOLDER_PATH, tile_name + ".tar.gz")
                    print(full_path)
                    
                    redis_instance.set(str(celery_task.request.id), 0)
                
                    if full_path.is_dir():
                        size = sum(f.stat().st_size for f in full_path.glob('**/*') if f.is_file())
                    else:
                        size = float(full_path.stat().st_size)
                    
                    def upload_callback(ct, bytes_transferred):
                        module_logger.info(ct.request.id)
                        current_bytes = int(redis_instance.get(str(ct.request.id)))
                        current_bytes += bytes_transferred
                        percent_complete = float(round((current_bytes / size) * 100, 2))
                        module_logger.info(bytes_transferred)
                        module_logger.info(percent_complete)

                        try:
                            ct.update_state(
                                task_id=str(ct.request.id),
                                state=states.STARTED,
                                meta={"upload": percent_complete}
                            )
                        except BaseException as e:
                            module_logger.info('Task isnt ready for updates to meta yet...')
                            module_logger.error(str(e))
                        
                        redis_instance.set(str(ct.request.id), int(current_bytes))

                    upload_callback_bound = partial(upload_callback, celery_task)
                    
                    upload_result = s3_helper.upload_single_file_to_s3(
                        tile_name + '.tar.gz', INPUT_BUCKET_NAME, callback=upload_callback_bound
                    )
                else:
                    download_result = TaskStatus(False, "There was a problem downloading the product.", download_result.message)
            else:
                download_result = TaskStatus(
                    False, "No results found on USGS API", None
                )

            return [("download", download_result, ""), ("upload", upload_result, "")]



@task(bind=True, time_limit=60*60*6, soft_time_limit=60*60*3)
def download_l8(self, params):
    """Periodic task to check for L8 requests, bundle and submit to USGS bulk service
    
    Steps:
        1. Fetch all jobs of type L8BulkDownload, status submitted
        2. Get tile name from each job
        3. Submit request to USGS in groups of 10 tiles
        4. Add job of type L8Batch to track each USGS batch:
            L8Batch job needs, USGS job id, tile name, and job id for each tile

    """
    params = params["options"]

    result_list = start_job(
        params["tile"],
        atmospheric_correction=params["ac"],
        api_source=params["api_source"],
        entity_id=params["entity_id"],
        celery_task=self,
    )

    module_logger.info(result_list)

    # UPDATE JOB MODEL HERE
    result_failures = []
    result_successes = []

    for result in result_list:
        module_logger.info(result)
        result_task_status = result[1]
        result_name = result[0]

        result_status = result_task_status.status
        result_message = result_task_status.message
        result_data = result_task_status.data

        if result_data:
            result_message += str(result_data)
        if not result_status:
            result_failures.append((result_name, result_message))

        result_successes.append((result_name, result_message))

    # sleep 30 seconds to allow check jobs to do its business
    time.sleep(30)

    if len(result_failures) != 0:
        raise TaskFailureException(result_failures)
    else:
        result_dict = {}
        for res in result_successes:
            result_dict[res[0]] = res[1]

        return result_dict