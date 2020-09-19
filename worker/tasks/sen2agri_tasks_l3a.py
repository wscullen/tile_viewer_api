""" This job runs Sen2agri atmospheric correction on a L1C Products previously downloaded.

Inputs: ImageryList
Optional Inputs: WindowSize (default 3)
                 Maja Version (default 3.2.2)
Outputs: Corrected product uploaded
        to S3 bucket

Dependencies: maccs-maja, sen2agri installation

Major Steps

1. Parse Imagery List into DateList

2. Create command for each date

    For each date
    3. Check if L2A results exist already on S3

    4. Download L1C and previously created L2A for all Dates but Date 0

    5. Run demmaccs.py command for that date

    6. Upload resulting L2A imagery to S3

    7. Clean up local

Notes:
In the future, a convertion to a COG tif (cloud optimized geotiff) would be a
good addition.
"""

# Create your tasks here
from __future__ import absolute_import, unicode_literals
import boto3
from botocore.client import Config
import botocore
from celery import group, shared_task, task
import click
import logging
import datetime
from django.db.models import Q
from django.conf import settings
from datetime import datetime
from datetime import timedelta
import json
import logging
import math
import multiprocessing
from pathlib import Path
import os
from osgeo import gdal
import re
import shutil
import subprocess
import sys
import tarfile
import time
import zipfile

from common.s3_utils import S3Utility
from common.utils import TaskStatus, unarchive, clean_up_folder, run_cmd

# from landsat_downloader import l8_downloader

from django.conf import settings


s3_helper = S3Utility(settings.S3_CONFIG, Path(settings.BASE_DIR, "working_folder"))

module_logger = logging.getLogger("worker.sen2agri.tasks")

INPUT_BUCKET_NAME = "sen2agri-l2a"
OUTPUT_BUCKET_NAME = "sen2agri-l3a"

WORKING_FOLDER_PATH = Path(settings.BASE_DIR, "working_folder")

STATUS_FOLDER_PATH = Path(WORKING_FOLDER_PATH, "status")
OUTPUT_FOLDER_PATH = Path(WORKING_FOLDER_PATH, "output")
INPUT_FOLDER_PATH = Path(WORKING_FOLDER_PATH, "input")


def write_result_to_status_folder(result_list, job_id):
    """After the job completes it should write out a json file with success info

    The Json should be written to the status folder, to be used by the parent
    process to report job failure or success to the server

    result_list should be a list of tuples with
    ('sub_task_name', False, 'result_message')

    file_name_format = JobStatus_YYYYMMDD_hhmm_job_id.json

    """
    date_now = datetime.datetime.now()
    date_string = date_now.strftime("%Y%m%d_%H%M")
    file_name = f"SimplifyS1Tif_{date_string}_{job_id}.json"
    file_full_path = Path(STATUS_FOLDER_PATH, file_name)

    with open(file_full_path, "w") as outfile:
        json.dump(result_list, outfile)


def find_l2a_path(l1c_path):
    module_logger.info(l1c_path)

    name = Path(l1c_path).name

    module_logger.info(name)

    sat = name[2]
    date = name.split("_")[2]
    orbit = name.split("_")[4]
    tile = name.split("_")[5]
    l2a_name = None
    regex_str = r"S2{}_MSIL2A_{}_N\d{{4}}_{}_{}_\d{{8}}T\d{{6}}.SAFE".format(
        sat, date, orbit, tile
    )
    # iterate over work dir, find a dir that matches the regex

    for thing in os.listdir(WORKING_FOLDER_PATH):
        module_logger.info(thing)
        search_result = re.search(regex_str, thing)
        module_logger.info(search_result)
        if search_result:
            return Path(WORKING_FOLDER_PATH, thing)

    return l2a_name


def check_l2a_imagery_exists(imagery_date, tile_name, aoi_name):
    """
    imagery date format '20180809'
    tile format = 'T14UNV'
    """

    logging.info("checking if imagery exists on s2")
    logging.info(imagery_date)
    logging.info(tile_name)

    search_expression = r"SENTINEL2[A|B]_{}-\d{{6}}-\d{{3}}_L2A_{}_C_V1-0".format(
        imagery_date, tile_name
    )
    logging.info(search_expression)

    # search_expresssion, object_prefix, bucket_name
    object_exists = s3_helper.check_object_exists_in_s3_wildcards(
        search_expression, str(Path(aoi_name, "sentinel2")), INPUT_BUCKET_NAME
    )
    logging.info("object_exists in l2a?")
    logging.info(object_exists)

    return object_exists


def check_if_l2a_imagery_exists_local(l1c_input):

    logging.info(l1c_input)
    name_parts = l1c_input.split("_")
    search_expression = r"SENTINEL2[A|B]_{}-\d{{6}}-\d{{3}}_L2A_{}_C_V1-0".format(
        name_parts[2][:8], name_parts[5]
    )
    logging.info(search_expression)
    result_path = None

    for folder in os.listdir(OUTPUT_FOLDER_PATH):
        search_result = re.search(search_expression, folder)
        logging.info(folder)
        if search_result:
            result_path = Path(OUTPUT_FOLDER_PATH, folder)

    logging.info(result_path)
    return result_path


def check_if_l2a_imagery_exists(imagery_date, l1c_name):

    logging.info("checking if imagery exists on s2")
    logging.info(l1c_name)
    name_parts = l1c_name.split("_")

    if imagery_date == None:
        imagery_date = name_parts[2][:8]

    search_expression = r"SENTINEL2[A|B]_{}-\d{{6}}-\d{{3}}_L2A_{}_C_V1-0".format(
        imagery_date, name_parts[1]
    )
    logging.info(search_expression)
    logging.info("checking if object exists in s3 with wildcards")

    # search_expresssion, object_prefix, bucket_name
    object_exists = s3_helper.check_object_exists_in_s3_wildcards(
        search_expression, "s2", "sen2agri-l2a"
    )

    return object_exists


def check_if_l2a_imagery_exists_local_date_tile(current_date, tile):

    logging.info(f"checking for local sen2agri for date {current_date} and tile {tile}")
    search_expression = r"SENTINEL2[A|B]_{}-\d{{6}}-\d{{3}}_L2A_{}_C_V1-0".format(
        current_date, tile
    )
    logging.info(search_expression)
    result_path = None

    for folder in os.listdir(WORKING_FOLDER_PATH):
        search_result = re.search(search_expression, folder)
        logging.info(folder)
        if search_result:
            result_path = Path(WORKING_FOLDER_PATH, folder)

    logging.info(result_path)
    return result_path


def find_l2a_xml_paths_in_working_folder(date_list, tile):

    path_list = []
    logging.info(date_list)

    # Example product
    # SENTINEL2A_20180521-173646-866_L2A_T14UNA_C_V1-0/
    for d in date_list:
        for product_dir in os.listdir(WORKING_FOLDER_PATH):
            regex_str = r"SENTINEL2[A|B]_{}-\d{{6}}-\d{{3}}_L2A_{}_C_V1-0".format(
                d, tile
            )

            logging.info(regex_str)
            search_result = re.search(regex_str, product_dir)

            if search_result:
                for f in os.listdir(Path(WORKING_FOLDER_PATH, product_dir)):
                    if f.find("MTD_ALL.xml") != -1:
                        path_list.append(Path(WORKING_FOLDER_PATH, product_dir, f))
                        break

    return path_list


def create_l3a_composite(
    synthdate, synth_halflength, tile, imagery_date_list, window_size
):
    """
        Major steps:

        2. Construct command including imagery
        3. Upload result to S3
    """
    logging.info("run composite command here\n\n\n\n\n")
    logging.info("----------------------------------------")

    path_list = find_l2a_xml_paths_in_working_folder(imagery_date_list, tile)

    logging.info(path_list)

    composite_command = f'/usr/bin/composite_processing.py --tileid {tile} --syntdate {synthdate} --synthalf {synth_halflength} --input {" ".join([str(p) for p in path_list])} --res 10 --outdir {OUTPUT_FOLDER_PATH} --bandsmap /usr/share/sen2agri/bands_mapping_s2_L8.txt --scatteringcoef /usr/share/sen2agri/scattering_coeffs_10m.txt'

    logging.info(composite_command)

    result = run_cmd(composite_command)

    return result


def get_imagery_in_window_from_synthdate(synthdate, all_dates, window_length=30):

    synthdate_date = datetime.datetime.strptime(synthdate, "%Y%m%d")
    synth_startperiod = synthdate_date - datetime.timedelta(days=window_length)
    synth_endperiod = synthdate_date + datetime.timedelta(days=window_length)
    synthwindow_dates = []
    for d in all_dates:
        date_obj = datetime.datetime.strptime(d, "%Y%m%d")

        if synth_startperiod <= date_obj <= synth_endperiod:
            logging.info("date is in window, adding to list")
            synthwindow_dates.append(d)

    return synthwindow_dates


@shared_task
def generate_l3a(imagery_list, window_size, aoi_name):

    # Determine dates
    all_dates = list(imagery_list.keys())

    # Determine dates on a per tile basis
    tile_dict = {}
    for d in all_dates:
        image_list = imagery_list[d]
        for image in image_list:
            # get the tile
            tile = image.split("_")[5]
            if tile in tile_dict:
                tile_dict[tile].append(d)
            else:
                tile_dict[tile] = []
                tile_dict[tile].append(d)

    logging.info(imagery_list)
    logging.info(tile_dict)

    # get date window length divide into 30 days
    start_date = datetime.strptime(all_dates[0], "%Y%m%d")
    end_date = datetime.strptime(all_dates[-1], "%Y%m%d")

    synthdates = []
    new_date = start_date

    logging.info((end_date - new_date).days)

    while (end_date - new_date).days > 30:
        new_date = new_date + timedelta(days=30)
        synthdates.append(new_date)

    logging.info(synthdates)

    synthdates_str = [d.strftime("%Y%m%d") for d in synthdates]

    logging.info(synthdates_str)

    result_list = []

    logging.info(window_size)

    synth_halflength = math.ceil(window_size / 2)

    # length of the synthesis in days (half)
    logging.info(synth_halflength)

    for tile in tile_dict.keys():
        # Fetch L2A imagery for tile (all dates)
        imagery_list = []
        for d in all_dates:

            l2a_exists = check_l2a_imagery_exists(d, tile, aoi_name)

            logging.info("intermediate_l2a_exists")
            logging.info(l2a_exists)

            if l2a_exists.status:
                logging.info(
                    f"\nFor date {d}, prev l2a exists {tile}, trying to download...\n"
                )

                l2a_download_result = s3_helper.download_l2a_from_s3(
                    str(Path(aoi_name, "sentinel2", l2a_exists[1])), INPUT_BUCKET_NAME
                )

                logging.info(l2a_download_result)
                if l2a_download_result.status:
                    imagery_list.append(d)
                else:
                    return TaskStatus(
                        False,
                        "There was a problem downloading the l2a imagery",
                        l2a_exists.message,
                    )

            else:
                return TaskStatus(False, "missing l2a imagery, job aborted.", None)

        for d in synthdates_str:
            logging.info(f"creating composite for tile {tile} and date {d}")
            result = create_l3a_composite(
                d, synth_halflength, tile, imagery_list, window_size
            )
            result_list.append(result)
            # find l3a result
            product_path = None

            for f in os.listdir(OUTPUT_FOLDER_PATH):
                if f.find("S2AGRI_L3A") != -1:
                    product_path = Path(OUTPUT_FOLDER_PATH, f)
                    break

            if product_path:
                upload_result = s3_helper.upload_unarchived_product_to_s3(
                    product_path, aoi_name, OUTPUT_BUCKET_NAME
                )
            else:
                upload_result = TaskStatus(
                    False,
                    "L3A product could not be found, there was a problem in L3A Creation",
                    None,
                )

            if upload_result.status:
                shutil.rmtree(product_path)

        clean_up_folder(WORKING_FOLDER_PATH)
        clean_up_folder(INPUT_FOLDER_PATH)
        clean_up_folder(OUTPUT_FOLDER_PATH)

        if upload_result.status:
            logging.info(upload_result)
            logging.info("created composite and uploaded to s3")

            result_list.append(TaskStatus(True, f"{tile} - {d}", ""))

        else:
            logging.info("there was a problem with uploading the composite product")
            result_list.append(
                TaskStatus(False, f"{tile} - {d}", upload_result.message)
            )

    return result_list


def start_l3a_job(imagery_list, job_id, aoi_name, window_size=3, maja_ver="3.2.2"):
    """Given the list of input imagery, start the job processes

    Major Steps:
    1. Generate date array from input imagery list
    2. Generate command for each date from 0 to n
    3. Run command for date 0
        3. Check if expected L2A output exists on S3 bucket, if not:
        3. Retrieve l1c imagery for date 0 from S3 bucket, if l1c imagery cannot be found, entire job fails, extract zip
        3. Run demmaccs.py for date 0
        3. Upload l2a output to S3 bucket
        3. Delete local working dir contents
    4. Run next command for date 1 to n
        4. Check if expected L2A exists on S3 bucket, if not:
        4.  Retrieve l1c imagery for date i from S3 bucket, if l1c imagery cannot be found, entire job fails, extract zip
        4. Retrieve l2a imagery for date i-1, date i-2, date i-3
        3. Run demmaccs.py for date i
        3. Upload l2a output to S3 bucket
        4. Delete local working dir contents
    5. If all commands complete successfully, report job complete
    6. Else report job failure

    imagery list format example:

    JSON:
      "parameters": {
            "aoi_name": "Test Area 2",
            "window_size": 3,
            "imagery_list": {
                "sentinel2": {
                    "20190613": [
                        "S2A_MSIL1C_20190613T182921_N0207_R027_T12UUA_20190613T220508"
                    ],
                    "20190616": [],
                    "20190618": [
                        "S2B_MSIL1C_20190618T182929_N0207_R027_T12UUA_20190618T220500"
                    ],
                    "20190621": [],
                    "20190623": [],
                    "20190626": [],
                    "20190628": [
                        "S2B_MSIL1C_20190628T182929_N0207_R027_T12UUA_20190628T221748"
                    ],
                    "20190701": [],
                    "20190703": [
                        "S2A_MSIL1C_20190703T182921_N0207_R027_T12UUA_20190703T220857"
                    ],
                    "20190706": [],
                    "20190718": [
                        "S2B_MSIL1C_20190718T182929_N0208_R027_T12UUA_20190718T220349"
                    ],
                    "20190721": []
                }
            }
    
    """
    conv_imagery_list = {}
    imagery_list = imagery_list["sentinel2"]

    for (key, value) in imagery_list.items():
        if len(value) > 0:
            conv_imagery_list[key] = []
            for img in value:
                # S2A_MSIL1C_20190703T182921_N0207_R027_T12UUA_20190703T2328
                conv_imagery_list[key].append(img)

    print(conv_imagery_list)

    # Todo: add checks for existing products (to prevent redundant work)
    task = generate_l3a.s(conv_imagery_list, window_size, aoi_name)

    logging.info(task)
    task_result = task.apply_async().get()
    json_result = json.dumps(task_result)

    return json_result
