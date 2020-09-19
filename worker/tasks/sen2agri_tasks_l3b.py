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
import celery
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
from landsat_downloader import l8_downloader

from django.conf import settings


s3_helper = S3Utility(settings.S3_CONFIG, Path(settings.BASE_DIR, "working_folder"))

module_logger = logging.getLogger("worker.sen2agri.tasks")

INPUT_BUCKET_NAME = "sen2agri-l2a"
OUTPUT_BUCKET_NAME = "sen2agri-l3b"

WORKING_FOLDER_PATH = Path(settings.BASE_DIR, "working_folder")

STATUS_FOLDER_PATH = Path(WORKING_FOLDER_PATH, "status")
OUTPUT_FOLDER_PATH = Path(WORKING_FOLDER_PATH, "output")
INPUT_FOLDER_PATH = Path(WORKING_FOLDER_PATH, "input")

L3B_MONODATE = "monodate"
L3B_NDAYSFITTED = "ndaysfitted"
L3B_ENDOFSEASON = "endofseason"


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


def find_l2a_xml_paths_in_folder(date_list, tile, folder):

    path_list = []
    logging.info(date_list)

    # Example product
    # SENTINEL2A_20180521-173646-866_L2A_T14UNA_C_V1-0/
    for d in date_list:
        for product_dir in os.listdir(folder):
            regex_str = r"SENTINEL2[A|B]_{}-\d{{6}}-\d{{3}}_L2A_{}_C_V1-0".format(
                d, tile
            )

            logging.info(regex_str)
            search_result = re.search(regex_str, product_dir)

            if search_result:
                for f in os.listdir(Path(folder, product_dir)):
                    if f.find("MTD_ALL.xml") != -1:
                        path_list.append(Path(folder, product_dir, f))
                        break

    return path_list


def create_l3b_monodate_single_date(imagery_date, tile, working_folder):
    """
        Major steps:

        2. Construct command including imagery
        3. Upload result to S3
    """
    logging.info("run lai command here\n\n\n\n\n")
    logging.info("----------------------------------------")

    path_list = find_l2a_xml_paths_in_folder([imagery_date], tile, working_folder)

    output_folder = Path(working_folder, "output")

    logging.info(path_list)

    lai_command = f'/usr/bin/lai_retrieve_processing.py --tileid {tile} --input {" ".join([str(p) for p in path_list])} --res 10 --outdir {output_folder} --rsrcfg /usr/share/sen2agri/rsr_cfg.txt --modelsfolder {working_folder} --generatemodel YES --generatemonodate YES --genreprocessedlai NO --genfittedlai NO'

    logging.info(lai_command)

    result = run_cmd(lai_command)

    return result


@shared_task
def create_l3b_monodate(current_date, tile, aoi_name, job_id):

    task_working_folder = celery.current_task.request.id

    WORKING_FOLDER_PATH = Path(settings.BASE_DIR, "working_folder", task_working_folder)
    OUTPUT_FOLDER_PATH = Path(WORKING_FOLDER_PATH, "output")
    os.mkdir(WORKING_FOLDER_PATH)
    os.mkdir(OUTPUT_FOLDER_PATH)

    s3_helper.WORKING_FOLDER_PATH = WORKING_FOLDER_PATH

    logging.info(f"Creating monodate for {current_date}, {tile} for {aoi_name}")

    l2a_exists = check_l2a_imagery_exists(current_date, tile, aoi_name)

    if l2a_exists.status:
        l2a_download_result = s3_helper.download_l2a_from_s3(
            str(Path(aoi_name, "sentinel2", l2a_exists.message)), INPUT_BUCKET_NAME
        )
    else:
        return TaskStatus(False, "missing l2a imagery, job aborted.", None)

    logging.info(l2a_download_result)

    if not l2a_download_result.status:
        return TaskStatus(
            False,
            f"Problem occurred downloading L2A Product for tile {tile} and date {current_date}",
            None,
        )

    result = create_l3b_monodate_single_date(current_date, tile, WORKING_FOLDER_PATH)

    if result.status:
        # get l3a tile path
        l3b_path = None
        for product_dir in os.listdir(OUTPUT_FOLDER_PATH):
            if (
                product_dir.find("S2AGRI_L3B") != -1
            ):  # S2AGRI_L3B_PRD_Snn_20190706T212746_A20180516T173418
                for tile_dir in os.listdir(
                    Path(OUTPUT_FOLDER_PATH, product_dir, "TILES")
                ):
                    if tile_dir.find("S2AGRI_L3B") != -1:
                        l3b_path = Path(
                            OUTPUT_FOLDER_PATH, product_dir, "TILES", tile_dir
                        )
        upload_result = s3_helper.upload_unarchived_product_to_s3(
            l3b_path, aoi_name, OUTPUT_BUCKET_NAME
        )

        shutil.rmtree(WORKING_FOLDER_PATH)

        return upload_result
    else:
        return result

    # old code for ndays fitted and end of season
    # for idx, d in enumerate(all_dates[:3]):
    #     low_index = idx - prev_n_days
    #     prev_n_dates = all_dates[low_index:idx]
    #     logging.info(prev_n_days)
    #     logging.info(prev_n_dates)

    #     if len(prev_n_dates) == 0:
    #         continue

    #     for prev_date in prev_n_days:
    #         # fetch l3b mono dates for dates
    #         # S2AGRI_L3B_A20180516T173418_T14UNV
    #         search_expression = r'S2AGRI_L3B_A{}T\d{{6}}_T{}'.format(prev_date, tile)
    #         logging.info(search_expression)
    #         object_exists = check_object_exists_in_s3_wildcards(search_expression, aoi_name, 'sen2agri-l3b')
    #         logging.info('Object exists?')
    #         logging.info(object_exists)

    #         if object_exists:
    #             download_l2a_from_s3(str(Path(aoi_name, object_exists)), 'sen2agri-l3b')
    #         else:
    #             return TaskStatus(False, 'missing l2a imagery, job aborted.', None)

    #     search_expression = r'SENTINEL2[A|B]_{}-\d{{6}}-\d{{3}}_L2A_T{}_C_V1-0'.format(d, tile)
    #     logging.info(search_expression)
    #     object_exists = check_object_exists_in_s3_wildcards(search_expression, 's2', 'sen2agri-l2a')
    #     logging.info('Object exists?')
    #     download_l2a_from_s3(str(Path('s2', object_exists)), 'sen2agri-l2a')

    #     fitted_result = create_l3b_ndaysfitted(d, tile)
    # result_list.append(fitted_result)

    # when all the mono dates are generated,
    # for each date,
    # depending on window size, get prev N LAI monodates
    # get date l2a input
    # run fitting l3b subprocess
    # upload result
    # clean output folder and working folder

    # find composite product in output folder and upload to s3
    # S2AGRI_L3A_PRD_Snn_20190630T000052_V20180615

    # Find all monodate products

    # create_l3b_ndaysfitted
    # for d in all_dates[:1]:
    #     fitted_result = create_l3b_ndaysfitted(d, tile)
    # result_list.append(fitted_result)
    # upload final result

    # upload_result = None
    # for product in os.listdir(OUTPUT_FOLDER_PATH):
    #     if product.find('S2AGRI_L3B') != -1:
    #         for tile_folder in os.listdir(Path(OUTPUT_FOLDER_PATH, product, 'TILES')):
    #             if tile_folder.find(tile) != -1:
    #                 upload_result = upload_unarchived_l2a_to_s3(Path(OUTPUT_FOLDER_PATH, product, 'TILES', tile_folder), 'example_aoi_name', 'sen2agri-l3b')
    #                 break

    # clean_up_output_folder()

    # if upload_result:
    #     logging.info(upload_result)
    #     logging.info('created composite and uploaded to s3')

    #     result_list.append(TaskStatus(True, f'{tile} - {d}', ''))

    # else:
    #     logging.info('there was a problem with uploading the composite product')
    #     result_list.append(TaskStatus(False, f'{tile} - {d}', ''))


def generate_l3b(imagery_list, aoi_name, prev_n_days, l3b_type, job_id):
    """
    l3b_processing_type = ('monodate', 'ndaysfitted', 'endofseason')
    """

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
    logging.info(aoi_name)

    if l3b_type == L3B_MONODATE:

        task_list = []

        for tile in tile_dict.keys():

            for d in all_dates:
                task = create_l3b_monodate.s(d, tile, aoi_name, job_id)
                task_list.append(task)

        task_group = group(task_list).apply_async()
        logging.info(task_list)

        task_result = task_group.get()

        return task_result


def start_l3b_job(
    imagery_list, aoi_name, l3b_type, job_id, prev_n_days=3, maja_ver="3.2.2"
):
    """Given the list of input imagery, start the job processes

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

    logging.info(conv_imagery_list)

    # Todo: add checks for existing products (to prevent redundant work)
    # def generate_l3b(imagery_list, aoi_name, prev_n_days, l3b_type):

    task = generate_l3b(conv_imagery_list, aoi_name, prev_n_days, l3b_type, job_id)

    logging.info(task)

    json_result = json.dumps(task)

    # def generate_l3b(imagery_list, aoi_name, prev_n_days, l3b_type)

    return json_result
