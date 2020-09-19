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
from celery import group, shared_task, task, states
import click
import logging
import datetime
from django.db.models import Q
from django.conf import settings
from datetime import datetime
import json
import logging
import multiprocessing
from pathlib import Path
import os
from osgeo import gdal
import re
import shutil
from subprocess import Popen, PIPE
import subprocess
import sys
import tarfile
import time
import zipfile

from django.db import transaction
from celery.result import AsyncResult

from jobmanager.celery import app

from common.s3_utils import S3Utility
from common.utils import TaskStatus, unarchive, clean_up_folder, TaskFailureException

from django.conf import settings


s3_helper = S3Utility(settings.S3_CONFIG, Path(settings.BASE_DIR, "working_folder"))

# create logger
module_logger = logging.getLogger(__name__)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
module_logger.addHandler(ch)


INPUT_BUCKET_NAME = "s2-l1c-archive"
OUTPUT_BUCKET_NAME = "sen2agri-l2a"

WORKING_FOLDER_PATH = Path(settings.BASE_DIR, "working_folder")

STATUS_FOLDER_PATH = Path(WORKING_FOLDER_PATH, "status")
OUTPUT_FOLDER_PATH = Path(WORKING_FOLDER_PATH, "output")
INPUT_FOLDER_PATH = Path(WORKING_FOLDER_PATH, "input")


def mp_run(command):
    env_cur = {
        "HOSTNAME": "centos7-nfs-pod",
        "NGINX_PORT": "tcp://10.111.177.182:80",
        "NGINX_PORT_80_TCP_PORT": "80",
        "LESSOPEN": "||/usr/bin/lesspipe.sh %s",
        "KUBERNETES_PORT": "tcp://10.96.0.1:443",
        "PATH": "/root/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "KUBERNETES_SERVICE_PORT": "443",
        "LANG": "en_US.UTF-8",
        "TERM": "xterm",
        "SHLVL": "1",
        "KUBERNETES_SERVICE_HOST": "10.96.0.1",
        "NGINX_PORT_80_TCP_PROTO": "tcp",
        "LD_LIBRARY_PATH": ":/usr/local/lib",
        "HOME": "/root",
        "NGINX_PORT_80_TCP": "tcp://10.111.177.182:80",
        "KUBERNETES_PORT_443_TCP_ADDR": "10.96.0.1",
        "NGINX_SERVICE_HOST": "10.111.177.182",
        "QT_GRAPHICSSYSTEM_CHECKED": "1",
        "KUBERNETES_SERVICE_PORT_HTTPS": "443",
        "_": "/usr/bin/python",
        "KUBERNETES_PORT_443_TCP_PROTO": "tcp",
        "OLDPWD": "/",
        "KUBERNETES_PORT_443_TCP": "tcp://10.96.0.1:443",
        "NGINX_SERVICE_PORT": "80",
        "PWD": "/root/Development/simple_worker",
        "KUBERNETES_PORT_443_TCP_PORT": "443",
        "LS_COLORS": "rs=0:di=01;34:ln=01;36:mh=00:pi=40;33:so=01;35:do=01;35:bd=40;33;01:cd=40;33;01:or=40;31;01:mi=01;05;37;41:su=37;41:sg=30;43:ca=30;41:tw=30;42:ow=34;42:st=37;44:ex=01;32:*.tar=01;31:*.tgz=01;31:*.arc=01;31:*.arj=01;31:*.taz=01;31:*.lha=01;31:*.lz4=01;31:*.lzh=01;31:*.lzma=01;31:*.tlz=01;31:*.txz=01;31:*.tzo=01;31:*.t7z=01;31:*.zip=01;31:*.z=01;31:*.Z=01;31:*.dz=01;31:*.gz=01;31:*.lrz=01;31:*.lz=01;31:*.lzo=01;31:*.xz=01;31:*.bz2=01;31:*.bz=01;31:*.tbz=01;31:*.tbz2=01;31:*.tz=01;31:*.deb=01;31:*.rpm=01;31:*.jar=01;31:*.war=01;31:*.ear=01;31:*.sar=01;31:*.rar=01;31:*.alz=01;31:*.ace=01;31:*.zoo=01;31:*.cpio=01;31:*.7z=01;31:*.rz=01;31:*.cab=01;31:*.jpg=01;35:*.jpeg=01;35:*.gif=01;35:*.bmp=01;35:*.pbm=01;35:*.pgm=01;35:*.ppm=01;35:*.tga=01;35:*.xbm=01;35:*.xpm=01;35:*.tif=01;35:*.tiff=01;35:*.png=01;35:*.svg=01;35:*.svgz=01;35:*.mng=01;35:*.pcx=01;35:*.mov=01;35:*.mpg=01;35:*.mpeg=01;35:*.m2v=01;35:*.mkv=01;35:*.webm=01;35:*.ogm=01;35:*.mp4=01;35:*.m4v=01;35:*.mp4v=01;35:*.vob=01;35:*.qt=01;35:*.nuv=01;35:*.wmv=01;35:*.asf=01;35:*.rm=01;35:*.rmvb=01;35:*.flc=01;35:*.avi=01;35:*.fli=01;35:*.flv=01;35:*.gl=01;35:*.dl=01;35:*.xcf=01;35:*.xwd=01;35:*.yuv=01;35:*.cgm=01;35:*.emf=01;35:*.axv=01;35:*.anx=01;35:*.ogv=01;35:*.ogx=01;35:*.aac=01;36:*.au=01;36:*.flac=01;36:*.mid=01;36:*.midi=01;36:*.mka=01;36:*.mp3=01;36:*.mpc=01;36:*.ogg=01;36:*.ra=01;36:*.wav=01;36:*.axa=01;36:*.oga=01;36:*.spx=01;36:*.xspf=01;36:",
        "NGINX_PORT_80_TCP_ADDR": "10.111.177.182",
    }

    logging.info(env_cur)
    env_cur[
        "PYTHONPATH"
    ] = "/usr/lib64/python27.zip:/usr/lib64/python2.7:/usr/lib64/python2.7/plat-linux2:/usr/lib64/python2.7/lib-tk:/usr/lib64/python2.7/lib-old:/usr/lib64/python2.7/lib-dynload:/usr/lib64/python2.7/site-packages:/usr/lib/python2.7/site-packages"
    env_cur["LD_LIBRARY_PATH"] = "$LD_LIBRARY_PATH:/usr/local/lib"
    env_cur["LANG"] = "en_US.UTF-8"

    with Popen(
        command, shell=True, stdout=PIPE, stderr=PIPE, bufsize=1, env=env_cur
    ) as sp:
        for line in sp.stdout:
            yield (line.decode("utf8").rstrip())


def run_demmaccs_cmd(cmd_string):
    logging.info(cmd_string)

    output = []
    errorOutput = []

    for line in mp_run(cmd_string):
        logging.info(line)
        output.append(line)
        if line.startswith("ERROR:"):
            errorOutput.append(line)

    logging.info("Done calling subprocess")

    if len(errorOutput) > 0:
        return TaskStatus(
            False, "Something went wrong trying to correct the product", errorOutput
        )

    return TaskStatus(True, "Product Corrected Successfully", None)


def get_dates_in_window(current_date, all_dates, window_size):
    """
    Calculate the dates in the current window by:
    current date
    all dates
    and window size

    if the dates in the window are more than 15 days away from the current
    date, they are removed.

    Also returns the list of dates before the current window so they can be erased

    return value
    (dates_in_window, dates_before_window)
    """

    current_date_index = all_dates.index(current_date)

    bottom_index = max(0, current_date_index - (window_size - 1))

    dates_in_window = all_dates[bottom_index:current_date_index]
    logging.info(f"current date: {current_date}")
    logging.info(f"dates window: {dates_in_window}")

    # remove dates that are too far away from the current date
    max_difference_in_days = 15
    current_date_object = datetime.strptime(current_date, "%Y%m%d")
    dates_to_remove = []
    for d in dates_in_window:
        date_object = datetime.strptime(d, "%Y%m%d")
        day_delta = current_date_object - date_object
        if day_delta.days > max_difference_in_days:
            dates_to_remove.append(d)

    # dates_in_window - dates_to_remove (we want the difference between the lists)
    # in set terms we want all the elements that are in list 1 and are NOT in list 2
    dates_in_window = [item for item in dates_in_window if item not in dates_to_remove]

    # remove local L2A Product not in date window (occur before window)
    dates_before_window = all_dates[0 : max(bottom_index - 1, 0)]

    return (dates_in_window, dates_before_window)


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


def get_l2a_products_for_window(dates_in_window, current_date, tile, aoi_name):
    
    
    prev_l2a_command_list = []  # list of tuples, tile name and path to tile

    l2a_download_results = []
    for d in dates_in_window:
        module_logger.info(f"Checking {d} of {dates_in_window}")
        # check locally if the L2A exists first
        l2a_exists_local = check_if_l2a_imagery_exists_local_date_tile(
            d, tile, WORKING_FOLDER_PATH
        )

        module_logger.info(l2a_exists_local)

        if l2a_exists_local:
            prev_l2a_command_list.append((tile, str(Path(WORKING_FOLDER_PATH))))
        else:
            intermediate_l2a_exists = check_l2a_imagery_exists(d, tile, aoi_name)

            module_logger.info(
                f"For {d} of {dates_in_window}, does the L2A exist? {intermediate_l2a_exists.status}"
            )

            if intermediate_l2a_exists.status:

                module_logger.info(
                    f"For date: {current_date}, previous L2A product on {d} exists. Attempting download."
                )

                l2a_download_result = s3_helper.download_l2a_from_s3(
                    str(Path(aoi_name, "sentinel2", intermediate_l2a_exists[1])),
                    OUTPUT_BUCKET_NAME,
                    destination_folder=Path(WORKING_FOLDER_PATH, tile),
                )

                module_logger.info(f"Download result: {l2a_download_result}")

                if not l2a_download_result.status:
                    l2a_download_results.append(l2a_download_result)

                else:
                    prev_l2a_command_list.append(
                        (tile, str(Path(WORKING_FOLDER_PATH, tile)))
                    )

    if len(l2a_download_results) > 0:
        return TaskStatus(
            False,
            f"Required prereq L2A Imagery has failed to download",
            [value.message for value in l2a_download_results],
        )
    else:
        return TaskStatus(
            True,
            f"Required prereq L2A imagery for {tile} in window {dates_in_window} downloaded successfully",
            prev_l2a_command_list,
        )


def create_l2a_command(
    unarchive_result, prev_l2a_command_list, working_folder, output_folder
):
    if len(prev_l2a_command_list) > 0:
        prev_tiles_string = " ".join([ele[0] for ele in prev_l2a_command_list])
        prev_tiles_path_string = " ".join(
            [str(ele[1]) for ele in prev_l2a_command_list]
        )
        demmaccs_command = f"/usr/bin/python2.7 /usr/share/sen2agri/sen2agri-demmaccs/demmaccs.py --working-dir {str(working_folder)} --prev-l2a-tiles {prev_tiles_string} --prev-l2a-products-paths {prev_tiles_path_string} --srtm /mnt/archive/srtm --swbd /mnt/archive/swbd --processes-number-dem 2 --gipp-dir /mnt/archive/gipp_maja --maccs-launcher /opt/maja/3.2.2/bin/maja {unarchive_result[2]} {output_folder} --delete-temp False"
    else:
        demmaccs_command = f"/usr/bin/python2.7 /usr/share/sen2agri/sen2agri-demmaccs/demmaccs.py --working-dir {str(working_folder)} --srtm /mnt/archive/srtm --swbd /mnt/archive/swbd --processes-number-dem 2 --gipp-dir /mnt/archive/gipp_maja --maccs-launcher /opt/maja/3.2.2/bin/maja {unarchive_result[2]} {output_folder} --delete-temp False"

    return demmaccs_command


def create_l2a_imagery(current_date, tile, all_dates, window_size, aoi_name):
    """ 
        current_date = current date we want l2a imagery for
        tile = current tile we want l2a imagery for
        date_window = previous dates before current date (depends on window size)

        Overall steps:

        Check tile and date combo for existence of L2A imagery

        if L2A exists, we return
        else
            create date_window from all_dates and window_size
            for date in date_window
                check date for L2a imagery
                if l2a exists 
                    download it
                    add to l2a demmaccs command list
                else
                    recurse into create_l2a_imagery(date, tile, all_dates, date_window_size)
            
            download l1c imagery for current_date

            if l2a_demmaccs command list is not empty, create demmacs command wiht prev l2a tiles
            run demmaccs for current date
            upload l2a imagery
    """

    module_logger.info(
        f"Creating an L2A product for {tile}, date {current_date} of {all_dates}."
    )

    final_l2_exists = check_l2a_imagery_exists(current_date, tile, aoi_name)

    module_logger.info(
        f"For {tile} and {current_date}, L2A product exists: {final_l2_exists.status}"
    )

    if final_l2_exists.status:

        return TaskStatus(True, "L2A found on S3 successfully", None)

    else:
        # Create the date window for the given current date
        dates_in_window, dates_before_window = get_dates_in_window(
            current_date, all_dates, window_size
        )

        # Delete the l2a products from before the current window
        for d in dates_before_window:
            l2a_exists_local = check_if_l2a_imagery_exists_local_date_tile(
                d, tile, WORKING_FOLDER_PATH
            )

            if l2a_exists_local:
                shutil.rmtree(l2a_exists_local)

        l1c_download_result = s3_helper.download_tile_from_s3_date_tile(
            current_date, tile, INPUT_BUCKET_NAME
        )

        module_logger.info(f"L1C product download successful: {l1c_download_result}")

        if l1c_download_result.status:

            unarchive_result = unarchive(
                Path(WORKING_FOLDER_PATH, l1c_download_result[2]), WORKING_FOLDER_PATH
            )

            module_logger.info(f"Unarchiving L1C result: {unarchive_result}")

            if unarchive_result.status:
                module_logger.info("Getting L2A Products for window...")
                # Get the L2A products for the current window
                fetch_prev_l2a_result = get_l2a_products_for_window(
                    dates_in_window, current_date, tile, aoi_name
                )

                if fetch_prev_l2a_result.status:

                    demmaccs_command = create_l2a_command(
                        unarchive_result,
                        fetch_prev_l2a_result.data,
                        WORKING_FOLDER_PATH,
                        OUTPUT_FOLDER_PATH,
                    )

                    demmaccs_result = run_demmaccs_cmd(demmaccs_command)

                    module_logger.info(
                        f"Atmospheric correction result: {demmaccs_result}"
                    )

                    if demmaccs_result.status:
                        module_logger.info("Attempting upload to S3.")

                        new_l2a_path = check_if_l2a_imagery_exists_local(
                            unarchive_result.data.split("/")[-1]
                        )
                        module_logger.info(f"New l2a local path is {new_l2a_path}")

                        if new_l2a_path:
                            module_logger.info(
                                "Local path for newly created L2A found, uploading to s3."
                            )

                            upload_result = s3_helper.upload_unarchived_product_to_s3(
                                new_l2a_path,
                                str(Path(aoi_name, "sentinel2")),
                                OUTPUT_BUCKET_NAME,
                            )

                            if upload_result.status:
                                shutil.rmtree(new_l2a_path)

                            return upload_result
                        else:
                            return TaskStatus(
                                False,
                                "Unable to find generated L2A product after atmospheric correction process.",
                                None,
                            )
                    else:
                        return demmaccs_result
                else:
                    return fetch_prev_l2a_result
        else:
            return l1c_download_result


def check_l2a_imagery_exists(imagery_date, tile_name, aoi_name):
    """
    imagery date format '20180809'
    tile format = 'T14UNV'
    """

    module_logger.warning(
        f"Checking if Sen2Agri L2A product exists for {imagery_date} and {tile_name}"
    )

    search_expression = r"SENTINEL2[A|B]_{}-\d{{6}}-\d{{3}}_L2A_T{}_C_V1-0".format(
        imagery_date, tile_name
    )

    module_logger.warning(f"Search expression: {search_expression}")

    # search_expresssion, object_prefix, bucket_name
    object_exists = s3_helper.check_object_exists_in_s3_wildcards(
        search_expression, str(Path(aoi_name, "sentinel2")), "sen2agri-l2a"
    )

    return object_exists


def generate_l2a_imagery(imagery_list, aoi_name, job_id, window_size=3):
    """
        Steps:
            Get dates from date window (current plus 2 previous)
            If date == date 0, no need to check other dates
            for each date, check if l2a exists, if it doesn't recursively call generate_l2a_imagery
            unless date 0, date 0 proceeds unless l1c is not available
    """
    module_logger.info("Inside generate l2a imagery")

    # Determine dates
    all_dates = list(imagery_list.keys())

    # Determine dates on a per tile basis
    tile_dict = {}
    for d in all_dates:
        image_list = imagery_list[d]
        for image in image_list:
            # get the tile
            tile = image.split("_")[5][1:]
            if tile in tile_dict:
                tile_dict[tile].append(d)
            else:
                tile_dict[tile] = []
                tile_dict[tile].append(d)

    module_logger.info(imagery_list)
    module_logger.info(tile_dict)

    task_list = []
    tile_list = []
    for tile in tile_dict.keys():
        module_logger.info("Submitting tasks for each tile and date list set...")
        task = create_l2a_imagery_for_tile.s(tile, all_dates, aoi_name, window_size)
        module_logger.info(task)
        task_list.append(task)
        tile_list.append(tile)

    module_logger.info("here?")
    task_results = group(task_list).apply_async()
    module_logger.info("here?")

    module_logger.info(task_list)
    module_logger.info(task_results)
    module_logger.info(task_results.children)

    module_logger.debug("Tasks for all tiles started atmos correction...")

    task_id_list = [task_result.task_id for task_result in task_results.children]

    module_logger.warning(task_id_list)
    module_logger.warning(zip(tile_list, task_id_list))

    return [[item[0], item[1]] for item in zip(tile_list, task_id_list)]


@app.task(bind=True)
def create_l2a_imagery_for_tile(self, tile, all_dates, aoi_name, window_size):
    result_dict = {}

    total_dates = len(all_dates)

    while len(all_dates) > 0:
        for idx, d in enumerate(all_dates):
            result = create_l2a_imagery(d, tile, all_dates, window_size, aoi_name)
            
            if not result.status:
                module_logger.info(
                    f"Possibly maja processing failed for date {d}, {result}, removing and starting again"
                )

                clean_up_folder(Path(WORKING_FOLDER_PATH, tile))
                raise TaskFailureException(result.msg)
                # if len(result.message) == 0:
                #     result_dict[d] = TaskStatus(
                #         False,
                #         "Most likely MAJA processing failed due to too many cloudy pixels on tile",
                #         None,
                #     )
                # else:
                #     result_dict[d] = result

                # break
            else:
                self.update_state(
                state=states.STARTED,
                meta={
                    "dates_completed": idx + 1,
                    "dates_total": total_dates,
                    "most_recent_date": d,
                },
            )

            clean_up_folder(Path(WORKING_FOLDER_PATH, tile))

            result_dict[d] = result
        else:
            break

        all_dates.remove(d)

    logging.info(f"Done creating imagery for all dates for tile {tile}")

    logging.info("cleaning up working folder")

    clean_up_folder(WORKING_FOLDER_PATH)

    final_result_dict = {tile: result_dict}
    # in case the task completes too quickly for check jobs
    time.sleep(30)

    return final_result_dict


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


def check_if_l2a_imagery_exists_local_date_tile(current_date, tile, folder_path):

    module_logger.debug(
        f"Checking for local Sen2Agri L2A  for {tile} -- {current_date}"
    )

    search_expression = r"SENTINEL2[A|B]_{}-\d{{6}}-\d{{3}}_L2A_{}_C_V1-0".format(
        current_date, tile
    )
    module_logger.debug(f"Search expression: {search_expression}")

    result_path = None

    for folder in os.listdir(folder_path):
        search_result = re.search(search_expression, folder)

        if search_result:
            result_path = Path(folder_path, folder)

    logging.info(result_path)
    return result_path


def start_l2a_job(imagery_list, job_id, aoi_name, window_size=3, maja_ver="3.2.2"):
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
    module_logger.info("Attempting to start L2A celery tasks...")

    conv_imagery_list = {}
    imagery_list = imagery_list["sentinel2"]
    module_logger.info("yo yo yo, dis working?!")

    for (key, value) in imagery_list.items():
        if len(value) > 0:
            conv_imagery_list[key] = []
            for img in value:
                # S2A_MSIL1C_20190703T182921_N0207_R027_T12UUA_20190703T232831
                #  usgs_name = f'L1C_{img_parts[5]}_A000000_{img_parts[2]}'
                conv_imagery_list[key].append(img)

    module_logger.info(conv_imagery_list)
    # Todo: add checks for existing products (to prevent redundant work)
    task_group = generate_l2a_imagery(conv_imagery_list, aoi_name, job_id, window_size)
    module_logger.info("done calling generate_l2a_imagery")
    module_logger.info(task_group)
    return task_group

