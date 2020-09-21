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
import logging
import multiprocessing
import os
from pathlib import Path
import re
import shutil
from subprocess import Popen, PIPE, STDOUT
import sys
import zipfile
import time

from functools import partial

# 3rd party modules
import boto3
from botocore.client import Config
import botocore
import click
from osgeo import gdal
import redis


# # Create your tasks here
from celery import shared_task, task, app, states
from celery.result import AsyncResult

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

# S3 Configuration
MAX_S3_ATTEMPTS = 3

# Internal Minio Config
INPUT_BUCKET_NAME = "s2-l1c-archive"
OUTPUT_BUCKET_NAME = "s2-l2a-products"

CONFIG_FILE = "config.yaml"
CONFIG_FILE_PATH = Path(settings.BASE_DIR, CONFIG_FILE)

WORKING_FOLDER_PATH = Path(settings.BASE_DIR, "working_folder")
STATUS_FOLDER_PATH = Path(WORKING_FOLDER_PATH, "status")

s3_helper = S3Utility(settings.S3_CONFIG, WORKING_FOLDER_PATH)

module_logger = logging.getLogger("worker.tasks.download_s2")

redis_instance = redis.StrictRedis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=0,
    password=settings.REDIS_PASS,
    decode_responses=True,
)

def download_using_landsat_downloader(tile_name):
    downloader = l8_downloader.L8Downloader(CONFIG_FILE_PATH, verbose=False)

    product_dict = {
        "platform_name": "Sentinel-2",
        "dataset_name": "SENTINEL_2A",
        "entity_id": tile_name,
        "name": tile_name,
    }

    query_dict = {"cloud_percent": 100}

    product_result = downloader.search_for_products_by_name(
        product_dict["dataset_name"], [tile_name], query_dict
    )
    product_type = "STANDARD"

    module_logger.debug(product_result)

    result = None

    if len(product_result) > 0:
        for product in product_result:
            result = downloader.download_product(
                product, product_type, directory=WORKING_FOLDER_PATH
            )

    module_logger.debug(result)

    return result


def run_atmos_correction(tile_fullpath, resolution=10):
    """Call sen2cor using L2A_PRocess
    """

    command_string = f"L2A_Process --resolution {resolution} {tile_fullpath}"

    module_logger.debug(command_string)

    output = []
    errorOutput = []

    for line in mp_run(command_string):
        module_logger.debug(line)
        output.append(line)
        if line.startswith("ERROR:"):
            errorOutput.append(line)
        elif line.find("L2A_Process: not found") != -1:
            errorOutput.append(line)
        elif (
            line.find(
                "'L2A_Process' is not recognized as an internal or external command"
            )
            != -1
        ):
            errorOutput.append(line)

    module_logger.info("Done calling subprocess")
    module_logger.debug(output)
    module_logger.debug(errorOutput)

    if len(errorOutput) > 0:
        return TaskStatus(
            False, "Something went wrong trying to correct the product", errorOutput
        )

    return TaskStatus(True, "Product Corrected Successfully", None)


def find_l2a_path(l1c_path):
    module_logger.debug(l1c_path)

    name = Path(l1c_path).name

    module_logger.debug(name)

    sat = name[2]
    date = name.split("_")[2]
    orbit = name.split("_")[4]
    tile = name.split("_")[5]
    l2a_name = None
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


def check_if_l1c_imagery_exists(imagery_date, l1c_name):

    module_logger.info("checking if imagery exists on s3")
    module_logger.info(l1c_name)

    search_expression = l1c_name
    module_logger.info(search_expression)
    module_logger.info("checking if object exists in s3 with wildcards")

    # search_expresssion, object_prefix, bucket_name
    object_exists = s3_helper.check_object_exists_in_s3_wildcards(
        search_expression, "", INPUT_BUCKET_NAME
    )

    return object_exists


def check_for_l2a_tile_esa(tile_name):
    """Use sentinel_downloader to query for L2A version of this tile"""

    s2_dl = s2_downloader.S2Downloader(CONFIG_FILE_PATH)

    query_dict = {"producttype": "S2MSI2A"}

    tile_name = tile_name.replace("L1C", "L2A")
    tile_name_parts = tile_name.split("_")
    tile_name_parts[3] = "*"
    tile_name_parts[6] = "*"

    search_name = "_".join(tile_name_parts)
    search_result = s2_dl.search_for_products_by_name(
        "Sentinel-2", [search_name], query_dict
    )

    module_logger.debug(search_result)
    if len(search_result) > 0:
        return search_result.popitem(last=False)[1]
    else:
        return None


def check_for_l1c_tile_esa(tile_name):
    """Use sentinel_downloader to query for L2A version of this tile"""

    s2_dl = s2_downloader.S2Downloader(CONFIG_FILE_PATH)

    query_dict = {"producttype": "S2MSI1C"}

    search_result = s2_dl.search_for_products_by_name(
        "Sentinel-2", [tile_name], query_dict
    )

    module_logger.debug(search_result)
    if len(search_result) > 0:
        return search_result.popitem(last=False)[1]
    else:
        return None


def check_for_l1c_tile_usgs(tile_name):
    # def search_for_products_by_name(self, dataset_name, product_name_list, query_dict, detailed=False, just_entity_ids=False, write_to_csv=False, call_count=0):
    downloader = l8_downloader.L8Downloader(CONFIG_FILE_PATH)

    query_dict = {"cloud_percent": 100}

    results = downloader.search_for_products_by_name(
        "SENTINEL_2A", [tile_name], query_dict
    )
    module_logger.debug(results)
    if len(results) > 0:
        return results[0]
    else:
        return None


def download_using_sentinel_downloader(tile_id, tile_name, celery_task=None):

    # def download_fullproduct(self, tile_id, tile_name, directory):
    s2_dl = s2_downloader.S2Downloader(CONFIG_FILE_PATH)

    if celery_task:

        def callback(progress_so_far, total_filesize, percentage_complete):
            module_logger.info('Celery task info in download callback:')
            module_logger.info(celery_task)
            module_logger.info(type(celery_task))
            module_logger.info(percentage_complete)

            result = AsyncResult(celery_task)
            info = result.info
            state = result.state
            module_logger.info('Celery task info and state:')
            module_logger.info(info)
            module_logger.info(state)
            celery_task.update_state(
                state=states.STARTED, meta={"download": percentage_complete}
            )

        download_result = s2_dl.download_fullproduct_callback(
            tile_id, tile_name, WORKING_FOLDER_PATH, callback
        )

    else:

        download_result = s2_dl.download_fullproduct(
            tile_id, tile_name, WORKING_FOLDER_PATH
        )

    module_logger.debug(download_result)

    return download_result


def check_if_l2a_imagery_exists(l1c_name):
    module_logger.info("checking if imagery exists on s3")
    module_logger.info(l1c_name)

    l1c_split = l1c_name.split("_")
    band = l1c_split[5][3]
    zone = l1c_split[5][1:3]
    zone_100km = l1c_split[5][4:]

    module_logger.debug(band)
    module_logger.debug(zone)
    module_logger.debug(zone_100km)

    # S2B_MSIL1C_20190708T151659_N0208_R025_T20TMS_20190708T184730'

    l1c_name_parts = l1c_name.split("_")

    search_expression = r"{}_MSIL2A_{}_N\d{{4}}_{}_{}_\d{{8}}T\d{{6}}\.SAFE".format(
        l1c_name_parts[0], l1c_name_parts[2], l1c_name_parts[4], l1c_name_parts[5]
    )
    module_logger.info(search_expression)
    module_logger.info("checking if object exists in s3 with wildcards")

    # search_expresssion, object_prefix, bucket_name
    object_exists = s3_helper.check_object_exists_in_s3_wildcards(
        search_expression,
        "/".join(["tiles", zone, band, zone_100km]),
        OUTPUT_BUCKET_NAME,
    )

    return object_exists


def start_job(
    tile_name,
    atmospheric_correction=False,
    ac_res=10,
    api_source="usgs_ee",
    entity_id=None,
    celery_task=None,
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

    download_result = TaskStatus(False, "", "")
    extract_result = TaskStatus(False, "", "")
    upload_result = TaskStatus(False, "", "")
    ac_result = TaskStatus(False, "", "")

    module_logger.info(
        f"Tile: {tile_name}, AC: {atmospheric_correction}, AC_RES: {ac_res}, API: {api_source}, Entity ID: {entity_id}, Celery Task: {celery_task}"
    )
    module_logger.debug(WORKING_FOLDER_PATH)
    module_logger.debug(STATUS_FOLDER_PATH)

    print(celery_task)

    if not atmospheric_correction:
        l1c_already_exists = check_if_l1c_imagery_exists(None, tile_name)

        if l1c_already_exists[0]:
            module_logger.info("l1c already exists, not doing atmospheric correction")

            result_list = [
                ("download", TaskStatus(True, "l1c already on s3", None), ""),
                ("upload", TaskStatus(True, "l1c already on s3", None), ""),
            ]

            return result_list
        else:
            # new logic for handling downloading
            # 1. check with sentinel_downloader if an L2A for the L1C name exists, if it does, download it
            #       might need to write a different extraction function for the different ESA named archive
            # 2. if no L2A exists on L2A, run query for L1C on USGS servers
            #   (verify that landsat_downloader supports ESA names, otherwise convert name, or search with a different mechanism)
            #   Download with landsat_downloader if L1C exists for tile
            # 3. if no L1C exists on USGS, query on ESA for L1C, if it is found download it using sentinel_downloader

            # IMPORTANT NOTES:
            # Might have to switch to long ESA naming for S2 archives (to guarentee unique name for tiles at beginning and end of datastrips)
            # Switch to long ESA S2 name will cause all sorts of havoc that needs to be addressed when working with archive (download, extract, upload to s3)

            esa_l1c_exists = check_for_l1c_tile_esa(tile_name)
            tile_id = esa_l1c_exists["uuid"]
            tile_name = esa_l1c_exists["title"]

            if esa_l1c_exists:

                download_result = download_using_sentinel_downloader(
                    tile_id, tile_name, celery_task=celery_task
                )

            else:
                usgs_l1c_exists = check_for_l1c_tile_usgs(tile_name)

                if usgs_l1c_exists:
                    download_result = download_using_landsat_downloader(
                        usgs_l1c_exists["name"]
                    )
                    if download_result[0]:
                        input_path = Path(download_result[2])
                        output_path = Path(
                            Path(download_result[2]).parent, tile_name + ".zip"
                        )

                        try:
                            os.rename(input_path, output_path)
                        except BaseException as e:
                            module_logger.error(e)

            if download_result[0]:
                full_path = Path(WORKING_FOLDER_PATH, tile_name + ".zip")
                
                size = 0

                if full_path.is_dir():
                    size = sum(f.stat().st_size for f in full_path.glob('**/*') if f.is_file())
                else:
                    size = float(full_path.stat().st_size)

                module_logger.info('Celery task info:')
                module_logger.info(celery_task)
                module_logger.info(type(celery_task))
                module_logger.info(str(celery_task.request.id))
                
                module_logger.info('Setting redis task download progress.')
                redis_instance.set(str(celery_task.request.id), 0)

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
                module_logger.info('Starting upload')
                upload_result = s3_helper.upload_single_file_to_s3(
                    tile_name + ".zip", INPUT_BUCKET_NAME, callback=upload_callback_bound
                )

            result_list = [
                ("download", download_result, ""),
                ("upload", upload_result, ""),
            ]

            clean_up_folder(WORKING_FOLDER_PATH)

            return result_list
    else:

        check_l2a_already_exists_s3 = check_if_l2a_imagery_exists(tile_name)
        module_logger.debug(check_l2a_already_exists_s3)
        if check_l2a_already_exists_s3[0]:
            download_result = TaskStatus(
                True,
                "L2A Product already exists on S3, no need for any further work",
                None,
            )

            result_list = [("download", download_result)]

            return result_list

            # 1. Query for L2A tile using landsat_downloader
        esa_l2a_exists = check_for_l2a_tile_esa(tile_name)

        if esa_l2a_exists:
            tile_id = esa_l2a_exists["uuid"]
            tile_name = esa_l2a_exists["title"]
            download_result = download_using_sentinel_downloader(
                tile_id, tile_name, celery_task=celery_task
            )
            module_logger.debug(download_result)
            extract_result = unarchive(Path(download_result[2]), WORKING_FOLDER_PATH)

            if extract_result[0]:
                module_logger.debug(extract_result)
                l2a_path = find_l2a_path(extract_result[2])

                # # upload_callback = ProgressPercentage(l2a_path, celery_task, celery_task_id=celery_task.request.id)
                # def upload_callback(bytes_transferred):
                #     module_logger.info(bytes_transferred)
                #     celery_task.update_state(
                #         state=states.STARTED, meta={"upload": bytes_transferred}
                #     )

                redis_instance.set(str(celery_task.id), 0)
                
                full_path = l2a_path

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
                    
                upload_result = s3_helper.upload_unarchived_product_to_s3_bucket(
                    l2a_path, OUTPUT_BUCKET_NAME, callback=upload_callback_bound
                )

            if upload_result[0]:
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

        else:
            local_download_result = s3_helper.download_tile_from_s3(
                tile_name, INPUT_BUCKET_NAME, WORKING_FOLDER_PATH
            )

            if not local_download_result[0]:

                # No L2A can be found on copernicus Scihub, try a query on EE
                usgs_l1c_exists = check_for_l1c_tile_usgs(tile_name)

                if usgs_l1c_exists:
                    download_result = download_using_landsat_downloader(
                        usgs_l1c_exists["name"]
                    )

                    if download_result[0]:
                        input_path = Path(download_result[2])
                        output_path = Path(
                            Path(download_result[2]).parent, tile_name + ".zip"
                        )

                        try:
                            os.rename(input_path, output_path)
                        except BaseException as e:
                            module_logger.error(e)
                else:
                    # no L1C found on usgs EE, try on Copernicus Scihub
                    esa_l1c_exists = check_for_l1c_tile_esa(tile_name)
                    tile_id = esa_l1c_exists["uuid"]
                    tile_name = esa_l1c_exists["title"]

                    if esa_l1c_exists:
                        download_result = download_using_sentinel_downloader(
                            tile_id, tile_name
                        )

            if download_result[0] or local_download_result[0]:

                module_logger.debug(download_result)
                module_logger.debug(local_download_result)
                module_logger.debug("should upload to s3")

                archive_path = Path(WORKING_FOLDER_PATH, tile_name + ".zip")

                # If it is a new tile, and not able to be pulled from the repo, upload it, with no consequence to the result reporting
                if download_result[0]:
                    s3_helper.upload_single_file_to_s3(tile_name + '.zip', OUTPUT_BUCKET_NAME)
                else:
                    download_result = local_download_result

                module_logger.info("Download was successful")
                extract_result = unarchive(archive_path, WORKING_FOLDER_PATH)

            if extract_result[0]:
                ac_result = run_atmos_correction(extract_result[2], resolution=ac_res)
                module_logger.debug(extract_result)
                if ac_result[0]:
                    # new version of sen2cor renames the outputs in funky ways, need to find the output L2A path
                    l2a_path = find_l2a_path(extract_result[2])

                    module_logger.info(str(celery_task.request.id))
                
                    redis_instance.set(str(celery_task.request.id), 0)

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

                    upload_result = s3_helper.upload_unarchived_product_to_s3_bucket(
                        l2a_path, OUTPUT_BUCKET_NAME, callback=upload_callback_bound
                    )

            if upload_result[0]:
                module_logger.info("overall job done successfully")

            extract_result = TaskStatus(
                extract_result[0], str(extract_result[1]), str(extract_result[2])
            )
            download_result = TaskStatus(
                download_result[0], str(download_result[1]), str(extract_result[2])
            )

            result_list = [
                ("download", download_result),
                ("ac", ac_result),
                ("extract", extract_result),
                ("upload", upload_result),
            ]

            module_logger.debug(result_list)

            clean_up_folder(WORKING_FOLDER_PATH)

            return result_list


@task(bind=True, time_limit=60*60*6, soft_time_limit=60*60*3)
def download_s2(self, params):
    """
    """

    params = params["options"]

    module_logger.info(download_s2.request.id)
    self.id = download_s2.request.id
    module_logger.info(self.request.id)
    module_logger.info(self.id)

    result_list = start_job(
        params["tile"],
        atmospheric_correction=params["ac"],
        ac_res=params["ac_res"],
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
