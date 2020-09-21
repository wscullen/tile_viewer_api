# # Create your tasks here
from celery import shared_task, task, states
from celery.task.control import inspect

from jobmanager.celery import app

# # from celery.exceptions import *
import logging
import time
from django.db.models import Q
from django.conf import settings
from datetime import datetime

from django.contrib.auth.models import User

from django.urls import path, reverse

from pathlib import Path

import os
import re
import shutil
import sys
import tarfile
import zipfile

# 3rd party modules
import boto3
from botocore.client import Config
import botocore
from tabulate import tabulate
import requests

from celery.result import AsyncResult

from landsat_downloader.l8_downloader import L8Downloader
from sentinel_downloader.s2_downloader import S2Downloader

import json

import logging

from common.s3_utils import S3Utility, ProgressPercentage
from common.utils import TaskStatus, unarchive, clean_up_folder, TaskFailureException

from worker.tasks.download_s2 import download_s2
from worker.tasks.download_l8 import download_l8, download_l8_bulk_order

from django.db import transaction

import redis

from api_v1.models import JobStatusChoice
from api_v1.models import JobPriorityChoice

redis_instance = redis.StrictRedis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=0,
    password=settings.REDIS_PASS,
    decode_responses=True,
)

MAX_S3_ATTEMPTS = 20

INPUT_BUCKET_NAME = "l8-l1c-archive"
OUTPUT_BUCKET_NAME = "l8-l2a-products"

WORKING_FOLDER_PATH = Path(settings.BASE_DIR, "working_folder")

s3_helper = S3Utility(
    settings.S3_CONFIG, WORKING_FOLDER_PATH, max_attempts=MAX_S3_ATTEMPTS
)

# create logger
module_logger = logging.getLogger("api_v1.tasks")

# Sample celery task
@shared_task(bind=True)
def add_numbers(self, x, y):
    module_logger.debug(x)
    module_logger.debug(y)
    result = x + y
    module_logger.debug(result)
    return result


@shared_task(bind=True)
def debug_shared_task(self, msg):
    module_logger.debug(msg)
    return msg


@app.task(ignore_result=True)
def check_offline_queues_for_all_users():
    """Periodic task that goes through each user's offline queue
    check if tehre is not an active item for the user
    pop an item from that users zset (if there is any)
    get the relevant info for the key in the zset,
    submit a download request to the esa api, if successful,
    set the key for the hm as the active key for the users queue"""
    from django.contrib.auth.models import User

    redis_instance = redis.StrictRedis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        db=0,
        password=settings.REDIS_PASS,
        decode_responses=True,
    )

    s2_downloader = S2Downloader(path_to_config=settings.CONFIG_PATH)

    users = User.objects.all()

    for user in users:
        module_logger.info(user.id)
        module_logger.info(user.username)
        user_name = user.username

        active_key_name = f"{user_name}:s2:active"
        queue_name = f"{user_name}:s2:queue"

        active_key = redis_instance.get(active_key_name)
        module_logger.info(active_key)

        if not active_key:
            key_list = redis_instance.zpopmin(queue_name, count=1)

            module_logger.info(key_list)

            if len(key_list) > 0:

                hm_key, count = key_list[0]

                module_logger.info(hm_key)

                if hm_key:
                    hashmap_dict = redis_instance.hgetall(hm_key)

                    module_logger.info(hashmap_dict)
                    # "online": False,
                    # "tile": product["name"],
                    # "ac": ac,
                    # "ac_res": ac_res,
                    # "api_source": product["api"],
                    # "entity_id": product["entity_id"],
                    request_succeeded = s2_downloader.request_offline_product(
                        hashmap_dict["entity_id"]
                    )
                    module_logger.info(hm_key)
                    if request_succeeded:
                        module_logger.info(
                            "Online request succeeded, setting active key"
                        )
                        redis_instance.set(active_key_name, hm_key)

                    else:
                        module_logger.info(
                            "Offline to Online request failed, putting the hm_key back in the zset"
                        )

                        redis_instance.zadd(queue_name, {hm_key: count})

    # module_logger.info("feck")


@app.task(ignore_result=True)
def check_jobs():
    """Periodic task that checks all jobs of status Submitted or Assigned
        gets the associated task id, and updates the job based on the task status
    
        1. Iterate over all jobs with status of Submitted or Assigned

        2. For Submitted jobs:
            Get task status using job.task_id
            If task is STARTED, set status to assigned, set assign_date
            If task is SUCCESS OR FAILURE, set status to completed, set assign_date,
                completed_date to now
            If task is SUCCESS set success to True, if task is FAILURE set success to false

        3. For Assigned jobs:
            Get task status using job.task_id
            If task is SUCCESS, set status to complete, set success to True, set completed_date
            If task is FAILURE, set status to complete, set success to False, set completed_date
    """

    from .models import Job
    from .models import JobStatusChoice
    from .models import JobPriorityChoice

    i = inspect()

    jobs = (
        Job.objects.select_for_update()
        .filter(
            (
                (
                    Q(job_type__exact="S2Download")
                    | Q(job_type__exact="L8Download")
                    | Q(job_type__exact="Sen2Agri_L2A")
                    | Q(job_type__exact="L8BatchJob")
                    | Q(job_type__exact="S2BatchDownload")
                    | Q(job_type__exact="L8BatchDownload")
                )
                & (
                    Q(status__exact=JobStatusChoice.SUBMITTED.value)
                    | Q(status__exact=JobStatusChoice.ASSIGNED.value)
                )
            )
        )
        .order_by("submitted")
    )

    with transaction.atomic():
        module_logger.info("Checking jobs... ===============================================\n\n")
        module_logger.info(f"Total jobs: {len(jobs)}")
        for job in jobs:
            module_logger.info("Job --------------------------------------------------\n")
            module_logger.info(f"Job id: {job.id}")
            if job.job_type in ["S2Download"]:
                res = AsyncResult(job.task_id)
                module_logger.info(f"Job's task id: {job.task_id}")

                task_state = res.state
                task_info = res.info
                module_logger.info(f"Task state: {task_state}")
                module_logger.info(f"Task info: {task_state}")
                now_datetime = datetime.now()

                if task_state == states.STARTED:

                    job.status = JobStatusChoice.ASSIGNED.value
                    job.info = {"task_progress": task_info}
                    job.assigned = now_datetime

                elif task_state == states.SUCCESS or task_state == states.FAILURE:

                    if job.status == JobStatusChoice.SUBMITTED.value:
                        job.assigned = now_datetime

                    job.status = JobStatusChoice.COMPLETED.value
                    job.completed = now_datetime
                    if not isinstance(res.info, Exception):
                        job.result_message = ",".join(
                            [f"{tup[0]}:{tup[1]}" for tup in res.info]
                        )
                    else:
                        job.result_message = str(res.info)

                    module_logger.info(job.result_message)

                    if task_state == states.SUCCESS:
                        job.success = True
                        current_info = job.info
                        if "task_progress" in current_info.keys():
                            current_info["task_progress"]["upload"] = 100.0
                            job.info = current_info

                job.save()

            elif job.job_type in ["L8Download"] and job.parameters["options"]["ac"]:
                module_logger.info(job.id)
                module_logger.info(job.task_id)
                res = AsyncResult(job.task_id)

                task_state = res.state
                module_logger.info(task_state)
                now_datetime = datetime.now()

                if task_state == states.STARTED:

                    job.status = JobStatusChoice.ASSIGNED.value
                    job.assigned = now_datetime

                elif task_state == states.SUCCESS or task_state == states.FAILURE:

                    if job.status == JobStatusChoice.SUBMITTED.value:
                        job.assigned = now_datetime

                    job.status = JobStatusChoice.COMPLETED.value
                    job.completed = now_datetime
                    job.success = True if task_state == states.SUCCESS else False

                job.save()

            elif job.job_type in ["Sen2Agri_L2A"]:
                # Steps:
                # check meta data for each associated task in the task_id list
                # update the task progress for each task id in the list
                # if all tasks in the list have status of finished, update the job state to completed.
                module_logger.info("This is a Sen2Agri_L2A job")

                task_id_list = job.info["task_ids"]

                if "task_progress" in job.info.keys():
                    task_status_dict = job.info["task_progress"]
                else:
                    task_status_dict = {}

                now_datetime = datetime.now()

                for tile, task_id in task_id_list:
                    module_logger.info(task_id)

                    res = AsyncResult(task_id)
                    task_state = res.state
                    module_logger.warning('TASK INFO: ')
                    module_logger.info(res.info)

                    if task_id not in task_status_dict.keys():
                        task_status_dict[task_id] = {}

                    if task_state == states.SUCCESS or task_state == states.FAILURE:
                        task_status_dict[task_id]["status"] = task_state
                        task_status_dict[task_id]["result"] = res.info
                        task_status_dict[task_id]["kwargs"] = res.kwargs
                        task_status_dict[task_id]["args"] = res.args
                        task_status_dict[task_id]["name"] = res.name
                    else:
                        task_status_dict[task_id]["status"] = task_state
                        task_status_dict[task_id]["progress"] = res.info
                        task_status_dict[task_id]["kwargs"] = res.kwargs
                        task_status_dict[task_id]["args"] = res.args
                        task_status_dict[task_id]["name"] = res.name

                # for key, value in task_status_dict.items():
                #     print(key)
                #     print(value)
                #     print(value["status"])

                all_statuses = [
                    value["status"] for key, value in task_status_dict.items()
                ]

                # Check for at lease one task being "Started"
                at_least_one_task_started = any(
                    [states.STARTED == task_status for task_status in all_statuses]
                )

                # Check for all tasks being finished
                all_tasks_finished_successfully = all(
                    [states.SUCCESS == task_status for task_status in all_statuses]
                )

                all_tasks_finished = all(
                    [
                        task_status == states.SUCCESS or task_status == states.FAILURE
                        for task_status in all_statuses
                    ]
                )

                module_logger.info(all_statuses)
                module_logger.info(at_least_one_task_started)
                module_logger.info(all_tasks_finished_successfully)

                new_info = job.info.copy()
                module_logger.info(res.info)
                new_info["task_progress"] = task_status_dict

                job.info = new_info

                if at_least_one_task_started:
                    job.status = JobStatusChoice.ASSIGNED.value
                    job.assigned = now_datetime

                if all_tasks_finished_successfully:
                    if job.status == JobStatusChoice.SUBMITTED.value:
                        job.assigned = now_datetime

                    job.status = JobStatusChoice.COMPLETED.value
                    job.completed = now_datetime
                    job.success = True
                elif all_tasks_finished:
                    if job.status == JobStatusChoice.SUBMITTED.value:
                        job.assigned = now_datetime

                    job.status = JobStatusChoice.COMPLETED.value

                    job.completed = now_datetime
                    job.success = False

                job.save()

            elif job.job_type in ["L8BatchJob"]:

                module_logger.info("This is a L8BatchJob")

                if job.status == JobStatusChoice.SUBMITTED.value:
                    downloader = L8Downloader(
                        path_to_config=settings.CONFIG_PATH, verbose=False
                    )

                    # Check order status
                    order_id = job.parameters["usgs_order_id"]
                    module_logger.debug(order_id)
                    order_ready = downloader.check_order_status(order_id)
                    module_logger.debug(order_ready)

                    if str(job.task_id) != "00000000-0000-0000-0000-000000000000":
                        res = AsyncResult(job.task_id)
                        task_state = res.state
                        now_datetime = datetime.now()
                        module_logger.info(res.info)

                        if task_state == states.STARTED:

                            job.status = JobStatusChoice.ASSIGNED.value
                            job.assigned = now_datetime
                            new_info = job.info.copy()
                            module_logger.info(res.info)

                            new_info["task_progress"] = res.info
                            job.info = new_info

                        elif (
                            task_state == states.SUCCESS or task_state == states.FAILURE
                        ):

                            if job.status == JobStatusChoice.SUBMITTED.value:
                                job.assigned = now_datetime

                            job.status = JobStatusChoice.COMPLETED.value
                            new_info = job.info.copy()
                            module_logger.info(res.info)

                            new_info["task_progress"] = res.info

                            job.completed = now_datetime
                            job.success = (
                                True if task_state == states.SUCCESS else False
                            )

                    else:
                        if order_ready:
                            task_id = l8_batch_download.delay(
                                order_id, str(WORKING_FOLDER_PATH)
                            )
                            job.info = {"order_status": "ready"}
                            job.task_id = str(task_id)
                        else:
                            module_logger.info(f"Order {order_id} not ready yet.")
                            job.info = {"order_status": "not ready"}

                elif job.status == JobStatusChoice.ASSIGNED.value:
                    module_logger.info("TASK RESULT:")
                    res = AsyncResult(job.task_id)
                    module_logger.info(res)
                    task_state = res.state
                    now_datetime = datetime.now()

                    if task_state == states.SUCCESS or task_state == states.FAILURE:

                        job.status = JobStatusChoice.COMPLETED.value
                        new_info = job.info.copy()
                        module_logger.info(str(res.info))

                        new_info["task_progress"] = str(res.info)
                        job.info = new_info
                        job.completed = now_datetime
                        job.success = True if task_state == states.SUCCESS else False

                        # Now update the associated jobs
                        tile_list = job.parameters["tile_list"]

                        for tile in tile_list:
                            module_logger.info(tile)
                            l8_job = Job.objects.select_for_update().get(
                                Q(pk=tile["job_id"])
                            )
                            l8_job.status = JobStatusChoice.COMPLETED.value
                            l8_job.completed = now_datetime
                            l8_job.success = job.success

                            l8_job.save()
                job.save()

            elif job.job_type in ["S2BatchDownload"]:
                
                module_logger.info(
                    "S2BatchDownload job, checking task status for each product id..."
                )

                updated_job = handle_s2_batch_job_check(job)
                updated_job.save()
            
            elif job.job_type in ["L8BatchDownload"]:
                module_logger.info(
                    "L8BatchDownload job, checking task status for each product id..."
                )

                updated_job = handle_l8_batch_job_check(job)
                module_logger.warning('WHAT THE WHAT')
                module_logger.warning(updated_job.id)

                module_logger.warning(type(updated_job))
                module_logger.warning('HOW IT DO DIS')

                save_result = updated_job.save()
                module_logger.info(save_result)


def s2_batch_job_not_started(job):
    """
    Check the task_id result, if ready, create task_progress and task_info keys for job.info
    """
    module_logger.info("Checking if the S2 Batch Job creation task has finished.")
    now_datetime = datetime.now()

    module_logger.info(job.id)
    module_logger.info(job.task_id)

    res = AsyncResult(job.task_id)

    task_state = res.state
    task_info = res.info
    module_logger.info(task_state)

    tile_ids = {}

    if task_state == states.SUCCESS:
        job_info = {}

        for key in task_info.keys():
            tile_ids[task_info[key]] = key

            job_info[key] = {"tile_id": task_info[key]}

            if key.startswith("redis"):
                job_info[key]["status"] = states.PENDING
            else:
                res = AsyncResult(key)
                sub_task_state = res.state

                if type(res.info) is not dict:
                    sub_task_info = str(res.info)
                else:
                    sub_task_info = res.info

                if task_state == states.SUCCESS:
                    job_info[key]["status"] = sub_task_state
                    job_info[key]["kwargs"] = res.kwargs
                    job_info[key]["args"] = res.args
                    job_info[key]["name"] = res.name

                    if sub_task_state == states.STARTED:
                        job_info[key]["progress"] = sub_task_info
                    else:
                        job_info[key]["result"] = sub_task_info

                module_logger.info(sub_task_state)
                module_logger.info(sub_task_info)
                module_logger.info("-----------------------------------")

        job.info["task_progress"] = job_info
        job.info["tile_ids"] = tile_ids

    elif task_state == states.FAILURE:
        job.status = JobStatusChoice.COMPLETED.value
        job.completed = now_datetime
        job.success = False

        module_logger.error("The S2 Batch Job creation task failed.")
        module_logger.error(f"Task id: {job.task_id}")
        module_logger.error(f"Error: {str(task_info)}")
        job.result_message = (
            f"The job failed during the creation task. {str(task_info)}"
        )

    return job


def s2_batch_job_handle_offline_product(
    redis_key, job_info_dict, tile_ids, job_owner_id
):
    """
     1. Check if there is an "active" key
     2. if there is an "active" key, check if this key matches it
            if this is the active key, check the Online status for this tile
                if this tile is Online
                    dispatch s2 download task (get task_id)
                    set the active key to none
                replace this key task_progress and tile_ids with the new task_id
            else there is no "active" key
            popmin on the zset for this users offline queue (this removes it from teh zset)
            set removed key from zset as active key
            make a single download request for this tile to initiate offline to online transfer
    """
    job_info_dict[redis_key]["status"] = states.PENDING
    user = User.objects.get(id=job_owner_id)
    user_name = user.username

    key_no_prefix = redis_key[5:]
    module_logger.info(key_no_prefix)

    active_key = redis_instance.get(f"{user_name}:s2:active")

    module_logger.info(f"current key without redis prefix: {key_no_prefix}")

    if active_key:
        module_logger.info(f"Active key found, checking... {active_key}")

        if active_key == key_no_prefix:
            module_logger.info(
                "The currently active key matches the key we are checking now"
            )

            tile_id = job_info_dict[redis_key]["tile_id"]

            module_logger.info(tile_id)
            module_logger.info(redis_key)

            tile_dict = redis_instance.hgetall(f"{user_name}:s2:{tile_id}")

            module_logger.info(tile_dict)

            s2_downloader = S2Downloader(path_to_config=settings.CONFIG_PATH)

            product_info = s2_downloader.get_product_info(tile_dict["entity_id"])

            module_logger.info(product_info)
            product_online = product_info["Online"]

            if product_online:
                module_logger.info("product is now online, submitting s2 download task")
                params = {
                    "options": {
                        "tile": tile_dict["tile"],
                        "ac": bool(int(tile_dict["ac"])),
                        "ac_res": [int(x) for x in tile_dict["ac_res"].split(",")],
                        "api_source": tile_dict["api_source"],
                        "entity_id": tile_dict["entity_id"],
                    }
                }

                task_id = download_s2.delay(params)
                module_logger.info(task_id)
                job_info_dict[str(task_id)] = {"tile_id": tile_id}

                del job_info_dict[redis_key]
                del tile_ids[tile_id]

                tile_ids[tile_id] = str(task_id)

                module_logger.info(tile_ids)
                module_logger.info(job_info_dict)

                redis_instance.delete(f"{user_name}:s2:active")
                redis_instance.delete(f"{user_name}:s2:{tile_id}")

            else:
                module_logger.info("product is not online yet")

    return (job_info_dict, tile_ids)


def s2_batch_job_handle_online_product(task_id, job_info_dict):
    res = AsyncResult(task_id)
    sub_task_state = res.state
    sub_task_info = res.info
    module_logger.info(sub_task_state)
    module_logger.info(sub_task_info)
    module_logger.info(job_info_dict)
    module_logger.info("-----------------------------------")

    if sub_task_state == states.SUCCESS or sub_task_state == states.FAILURE:
        job_info_dict[task_id]["status"] = sub_task_state
        job_info_dict[task_id]["result"] = str(res.info)
        job_info_dict[task_id]["kwargs"] = res.kwargs
        job_info_dict[task_id]["args"] = res.args
        job_info_dict[task_id]["name"] = res.name

        if (
            sub_task_state == states.SUCCESS
            and "progress" in job_info_dict[task_id].keys()
        ):
            if not job_info_dict[task_id]["progress"]:
                job_info_dict[task_id]["progress"] = {}

            job_info_dict[task_id]["progress"]["upload"] = 100.0
            job_info_dict[task_id]["progress"]["download"] = 100.0

    else:
        module_logger.info('sub task state is not success or failure')
        job_info_dict[task_id]["status"] = sub_task_state

        task_progress = {}
        module_logger.info(sub_task_state)
        module_logger.info(states.PENDING)
        if sub_task_state != states.PENDING:
            module_logger.info("sub task state is not pending")
            for key in res.info.keys():
                if key == 'upload':
                    task_progress[key] = res.info['upload']
                if key == 'download':
                    task_progress[key] = res.info['download']
        
        
        if "progress" in job_info_dict[task_id].keys() and type(job_info_dict[task_id]["progress"]) == dict:
            job_info_dict[task_id]["progress"].update(task_progress)
        else:
            job_info_dict[task_id]["progress"] = task_progress
        
        if sub_task_info is not None:
            job_info_dict[task_id]["kwargs"] = res.kwargs
            job_info_dict[task_id]["args"] = res.args
            job_info_dict[task_id]["name"] = res.name

    return job_info_dict


def s2_batch_job_in_progress(job):
    """ S2 Batch Job was submitted successfully, checking progress"""

    module_logger.info("Checking status of in progress S2 Batch Job")
    now_datetime = datetime.now()

    job_info = job.info

    job_info_dict = job_info["task_progress"].copy()
    tile_ids = job_info["tile_ids"].copy()
    module_logger.info("Iterating over task progress keys...")

    for key in job_info["task_progress"].keys():
        module_logger.info(f"Key: {key}")

        if key.startswith("redis"):
            module_logger.info("Found redis key, handling currently offline product")

            job_info_dict, tile_ids = s2_batch_job_handle_offline_product(
                key, job_info_dict, tile_ids, job.owner_id
            )

            module_logger.info("Results from offline product check")
            module_logger.info(job_info_dict)
            module_logger.info(tile_ids)
            if job_info_dict:
                job.info["task_progress"] = job_info_dict
            if tile_ids:
                job.info["tile_ids"] = tile_ids

        else:
            module_logger.info("Found task id keyh, handling online product")
            job_info_dict = s2_batch_job_handle_online_product(key, job_info_dict)
            module_logger.info("Results from online product check")
            module_logger.info(job_info_dict)
            if job_info_dict:
                job.info["task_progress"] = job_info_dict

    all_statuses = [value["status"] for key, value in job_info_dict.items()]

    # Check for at lease one task being "Started"
    at_least_one_task_started = any(
        [states.STARTED == task_status for task_status in all_statuses]
    )

    # Check for all tasks being finished
    all_tasks_finished_successfully = all(
        [states.SUCCESS == task_status for task_status in all_statuses]
    )

    all_tasks_finished = all(
        [
            task_status == states.SUCCESS or task_status == states.FAILURE
            for task_status in all_statuses
        ]
    )

    module_logger.info("Task statuses:")
    module_logger.info(f"At least one task started: {at_least_one_task_started}")
    module_logger.info(f"All tasks finished: {all_tasks_finished}")
    module_logger.info(
        f"All tasks finished SUCCESSFULLY: {all_tasks_finished_successfully}"
    )

    if at_least_one_task_started:
        job.status = JobStatusChoice.ASSIGNED.value
        job.assigned = now_datetime

    if all_tasks_finished_successfully:
        if job.status == JobStatusChoice.SUBMITTED.value:
            job.assigned = now_datetime

        job.status = JobStatusChoice.COMPLETED.value
        job.completed = now_datetime
        job.success = True
        send_email(job)

    elif all_tasks_finished:
        if job.status == JobStatusChoice.SUBMITTED.value:
            job.assigned = now_datetime

        job.status = JobStatusChoice.COMPLETED.value

        job.completed = now_datetime
        job.success = False

        send_email(job)

    return job


def handle_s2_batch_job_check(job):

    module_logger.info(f'Job id: {job.id}')

    if "task_progress" in job.info.keys():
        module_logger.info("S2 batch job in progress...")
        updated_job = s2_batch_job_in_progress(job)
    else:
        module_logger.info("S2 batch job NOT started...")
        updated_job = s2_batch_job_not_started(job)

    return updated_job

def handle_l8_batch_job_check(job):
    module_logger.info(f"Checking L8BatchDownload job id {job.id}\n\n")

    if "task_progress" in job.info.keys():
        module_logger.info("Task in progress.")
        updated_job = l8_batch_job_in_progress(job)
    else:
        module_logger.info("Task not started.")
        updated_job = l8_batch_job_not_started(job)

    return updated_job

def l8_batch_job_not_started(job):
    """
    Check the task_id result, if ready, create task_progress and task_info keys for job.info
    """
    module_logger.info("Checking if the L8 Batch Download creation task has finished.")
    now_datetime = datetime.now()

    module_logger.info(job.id)
    module_logger.info(job.task_id)

    res = AsyncResult(job.task_id)

    task_state = res.state
    task_info = res.info
    module_logger.info(task_state)
    module_logger.info(task_info)

    tile_ids = {}

    if task_state == states.SUCCESS:
        job_info = {}

        for key in task_info.keys():
            task_info_value = task_info[key]
            
            if isinstance(task_info_value, str):
                tile_ids[task_info_value] = key

                job_info[key] = {"tile_id": task_info_value}

               
                res = AsyncResult(key)
                sub_task_state = res.state

                if type(res.info) is not dict:
                    sub_task_info = str(res.info)
                else:
                    sub_task_info = res.info

                
                job_info[key]["status"] = sub_task_state
                job_info[key]["kwargs"] = res.kwargs
                job_info[key]["args"] = res.args
                job_info[key]["name"] = res.name

                if sub_task_state == states.STARTED:
                    if sub_task_info:
                        job_info[key]["progress"] = sub_task_info
                    else:
                        job_info[key]["progress"] = {}
                else:
                    if sub_task_info:
                        job_info[key]["result"] = sub_task_info
                    else:
                        job_info[key]["result"] = None

                module_logger.info(sub_task_state)
                module_logger.info(sub_task_info)
                module_logger.info("-----------------------------------")
            else:
                # the value for this key is a list of tile ids, that means we are dealing with the bulk
                # download service
                # task_info_value is a list of tile names

                tile_list = job.parameters['tile_list']
                tile_id_list = []
                for tile_name in [tile['name'] for tile in task_info_value]:
                    for tile in tile_list:
                        if tile['name'] == tile_name:
                            tile_id = tile['tile_id']
                            tile_ids[tile_id] = key
                            tile_id_list.append(tile_id)
                
                if key != "imagery_exists_on_s3":
                    job_info[key] = {"tile_id_list": tile_id_list, "status": states.PENDING}
                else:
                    job.info["imagery_on_s3"] = task_info_value

                job.info["task_progress"] = job_info
                job.info["tile_ids"] = tile_ids

    elif task_state == states.FAILURE:
        job.status = JobStatusChoice.COMPLETED.value
        job.completed = now_datetime
        job.success = False

        module_logger.error("The L8 Batch Job creation task failed.")
        module_logger.error(f"Task id: {job.task_id}")
        module_logger.error(f"Error: {str(task_info)}")
        job.result_message = (
            f"The job failed during the creation task. {str(task_info)}"
        )

    return job


def l8_batch_job_in_progress(job):
    """ L8 Batch Job was submitted successfully, checking progress"""

    now_datetime = datetime.now()

    job_info = job.info

    job_info_dict = job_info["task_progress"].copy()

    for key in job_info_dict.keys():

        # check for bulk download  id here, if bulk, handle check differently
        current_task_dict = job_info_dict[key]
        if "tile_id_list" in current_task_dict:
            job_info_dict = l8_batch_job_handle_bulk_order(key, job_info_dict)
            module_logger.warning(job_info_dict)
            if job_info_dict:
                job.info["task_progress"] = job_info_dict
        else:
            module_logger.info(f"Key: {key}")

            job_info_dict = l8_batch_job_handle_online_product(key, job_info_dict)
            module_logger.info("Results from online product check")
            module_logger.warning(job_info_dict)
            if job_info_dict:
                job.info["task_progress"] = job_info_dict
    
    all_statuses = [value["status"] for key, value in job_info_dict.items()]

    module_logger.info(f'All task statuses: {all_statuses}')

    # Check for at lease one task being "Started"
    at_least_one_task_started = any(
        [states.STARTED == task_status for task_status in all_statuses]
    )

    # Check for all tasks being finished
    all_tasks_finished_successfully = all(
        [states.SUCCESS == task_status for task_status in all_statuses]
    )

    all_tasks_finished = all(
        [
            task_status == states.SUCCESS or task_status == states.FAILURE
            for task_status in all_statuses
        ]
    )

    module_logger.info("Task statuses:")
    module_logger.info(f"At least one task started: {at_least_one_task_started}")
    module_logger.info(f"All tasks finished: {all_tasks_finished}")
    module_logger.info(
        f"All tasks finished SUCCESSFULLY: {all_tasks_finished_successfully}"
    )

    if at_least_one_task_started:
        job.status = JobStatusChoice.ASSIGNED.value
        job.assigned = now_datetime

    if all_tasks_finished_successfully:
        if job.status == JobStatusChoice.SUBMITTED.value:
            job.assigned = now_datetime

        job.status = JobStatusChoice.COMPLETED.value
        job.completed = now_datetime
        job.success = True
        send_email(job)

    elif all_tasks_finished:
        if job.status == JobStatusChoice.SUBMITTED.value:
            job.assigned = now_datetime

        job.status = JobStatusChoice.COMPLETED.value

        job.completed = now_datetime
        job.success = False

        send_email(job)

    return job


def l8_batch_job_handle_bulk_order(order_id, job_info_dict):

    # check if the order is ready, then submit a new download task
    module_logger.info('Bulk order...')
    downloader = L8Downloader(path_to_config=settings.CONFIG_PATH, verbose=False)

    module_logger.info(f'order id: {order_id}')
    module_logger.info(f'job info dict: {job_info_dict}')

    current_order_dict = job_info_dict[order_id]

    if 'download_task_id' not in current_order_dict:
        module_logger.info('Did not find "download_task_id", checking if order is ready for download yet')
        order_ready = downloader.check_order_status(order_id)
        module_logger.info(f'order ready: {order_ready}')
        if order_ready:
            # Submit the download task here
            module_logger.info('Submitting download task...')
            download_task_id = download_l8_bulk_order.delay(order_id)
            current_order_dict["download_task_id"] = str(download_task_id)
            module_logger.info(f"Download task id: {download_task_id}")
        else:
            if order_ready is None:
                module_logger.info('There was a problem trying to check the order...')
            else:
                module_logger.info('Order is not ready yet.')
    else:
        module_logger.info('Found a task id, checking the download task status')
        # Check the previously submitted download task metadata here
        download_task_id = current_order_dict["download_task_id"]
        module_logger.info(f'download task id: {download_task_id}')
        res = AsyncResult(download_task_id)
        sub_task_state = res.state
        sub_task_info = res.info

        module_logger.info(f'sub task state: {sub_task_state}')
        module_logger.info(f'sub task info: {sub_task_info}')
        module_logger.info(f'current order dict: {current_order_dict}')
        
        if sub_task_state == states.SUCCESS or sub_task_state == states.FAILURE:
            module_logger.info(f'sub task is {sub_task_state}')
            
            module_logger.info(res)
            current_order_dict["status"] = sub_task_state
            current_order_dict["result"] = str(sub_task_info)
            current_order_dict["kwargs"] = getattr(res, 'kwargs', None)
            current_order_dict["args"] = getattr(res, 'args', None)
            current_order_dict["name"] = getattr(res, 'name', None)
            module_logger.info("current order dict: ")
            module_logger.info(current_order_dict)
            progress_dict =  current_order_dict.get('progress', {})
            module_logger.info('progress dict:')
            module_logger.info(progress_dict)

            for key in progress_dict.keys():
                module_logger.info(f"key: {key}")
                single_product_progress = progress_dict[key]
                
                if sub_task_state == states.SUCCESS:
                    single_product_progress["status"] = states.SUCCESS
                    single_product_progress["upload"] = 100.0
                    single_product_progress["download"] = 100.0

                progress_dict[key].update(single_product_progress)


            current_order_dict["progress"] = progress_dict

        else:
            current_order_dict["status"] = sub_task_state
            
            if sub_task_info:
                current_order_dict["kwargs"] = getattr(res, 'kwargs', None)
                current_order_dict["args"] = getattr(res, 'args', None)
                current_order_dict["name"] = getattr(res, 'name', None)

                # "espa-ss.cullen@uleth.ca-06102020-022540-544": {
                #     "args": null,
                #     "name": null,
                #     "kwargs": null,
                #     "result": "[['download', [True, 'Downloading finished', '/code/working_folder/LC08_L1TP_041025_20190717_20190731_01_T1']], ['extract', [True, 'LC08_L1TP_041025_20190717_20190731_01_T1', '/code/working_folder/LC08_L1TP_041025_20190717_20190731_01_T1']], ['upload', [True, 'All files uploaded', '']]]",
                #     "status": "SUCCESS",
                #     "progress": {
                #         "LC08_L1TP_041025_20190717_20190731_01_T1": {
                #             "download": 100,
                #             "file_size": 455734439
                #         }
                #     },
                #     "tile_id_list": [
                #         "64795e41-710f-49ea-a45c-80de25e84921"
                #     ],
                #     "download_task_id": "e810ca8a-a06e-4989-94ff-a50c48a34a83"
                
                progress_dict = current_order_dict.get("progress", sub_task_info)
                
                module_logger.info(progress_dict)

                for key in sub_task_info.keys():
                    module_logger.info(f"Key: {key}")
                    try:
                        single_product_progress = sub_task_info[key]

                        module_logger.info(single_product_progress)
                        
                        if "download" in single_product_progress.keys():
                            single_product_progress['status'] = states.STARTED
                        
                        if "upload" in single_product_progress.keys() and single_product_progress["upload"] > 99.0:
                            single_product_progress["upload"] = 100.0
                        
                        if "download" in single_product_progress.keys() and single_product_progress["download"] > 99.0:
                            single_product_progress["download"] = 100.0

                        if (("upload" in single_product_progress.keys() and single_product_progress["upload"] > 99.0) and
                            ("download" in single_product_progress.keys() and single_product_progress["download"] > 99.0)):
                            single_product_progress['status'] = states.SUCCESS

                        if key in progress_dict.keys():
                            progress_dict[key].update(single_product_progress)
                        else:
                            progress_dict[key] = single_product_progress

                    except BaseException as e:
                        module_logger.error(str(e))
                        module_logger.error('Something went wrong trying to update the progress dictionary.')

                current_order_dict["progress"] = progress_dict
                module_logger.info("new current order dict")
                module_logger.info(current_order_dict)
            else:
                module_logger.info("The bulk download task has been submitted but has not been assigned to a worker yet.")

    job_info_dict[order_id] = current_order_dict

    module_logger.info(f'Updated job info dict: {job_info_dict}')

    return job_info_dict


def l8_batch_job_handle_online_product(task_id, job_info_dict):
    res = AsyncResult(task_id)
    sub_task_state = res.state
    sub_task_info = res.info

    module_logger.warning('TASK INFO: ')
    module_logger.info(sub_task_info)
    module_logger.info(sub_task_state)

    if sub_task_state == states.SUCCESS or sub_task_state == states.FAILURE:
        job_info_dict[task_id]["status"] = sub_task_state
        job_info_dict[task_id]["result"] = str(res.info)
        job_info_dict[task_id]["kwargs"] = res.kwargs
        job_info_dict[task_id]["args"] = res.args
        job_info_dict[task_id]["name"] = res.name

        if (
            sub_task_state == states.SUCCESS
            and "progress" in job_info_dict[task_id].keys()
        ):
            if not job_info_dict[task_id]["progress"]:
                job_info_dict[task_id]["progress"] = {}

            job_info_dict[task_id]["progress"]["upload"] = 100.0
            job_info_dict[task_id]["progress"]["download"] = 100.0

    else:
        job_info_dict[task_id]["status"] = sub_task_state
        task_progress = {}
        module_logger.info(sub_task_info.keys())
        for key in list(sub_task_info.keys()):
            if key == 'upload':
                task_progress[key] = sub_task_info['upload']
            if key == 'download':
                task_progress[key] = sub_task_info['download']
        
        if type(job_info_dict[task_id]["progress"]) == dict:
            job_info_dict[task_id]["progress"].update(task_progress)

        job_info_dict[task_id]["kwargs"] = res.kwargs
        job_info_dict[task_id]["args"] = res.args
        job_info_dict[task_id]["name"] = res.name

    module_logger.info(sub_task_state)
    module_logger.info(sub_task_info)
    module_logger.info("-----------------------------------")

    return job_info_dict


def send_email(job):
    tabulate_header = ["Tile Name", "API", "API Tile ID", "Result"]

    module_logger.info(tabulate_header)

    tile_list = job.parameters["tile_list"]
    parameters = job.parameters
    aoi_name = job.aoi_name

    job_type = job.job_type

    tile_ids = job.info["tile_ids"]
    task_progress = job.info["task_progress"]

    tabulate_list = [
        [t["name"], t["api"], t["entity_id"], t["tile_id"]] for t in tile_list
    ]

    for idx, tl in enumerate(tabulate_list):
        task_id = tile_ids[tl[3]]
        task_prog = task_progress[task_id]
        status = task_prog["status"]
        tl.append(status)
        del tl[3]

    module_logger.info(tabulate_list)
    user = User.objects.get(id=job.owner_id)
    user_email = user.email

    mail_subject = f"{job_type} Job Completed ({job.id})"

    if job.success:
        mail_text = f'<html><body><p style="color: green">Great news! Your {job_type} job has been completed successfully!</p>\n\n'
    else:
        mail_text = f'<html><body><p style="color: red">Bad news! Your {job_type} job has failed.</p>\n\n'

    job_url = f'{settings.BASE_URL}{reverse("job-detail", args=[job.id])}'

    mail_text += f'<p><a href="{job_url}">Click here to view full job details (requires authentication)</a><p>'

    mail_text += f"<pre>Job ID: {job.id}\nAOI Name: {aoi_name}\nSubmitted: {str(job.submitted)}\n"
    mail_text += f"Completed: {str(job.completed)}\n"
    mail_text += f"Params:\n"
    mail_text += f"ac: {parameters['ac']}\nac_res: {parameters['ac_res']}\n\n"
    mail_text += (
        tabulate(tabulate_list, headers=tabulate_header) + "</pre></body></html>"
    )
    module_logger.info(user_email)
    module_logger.info(mail_text)
    result = requests.post(
        settings.MAILGUN_API_URL,
        auth=("api", settings.MAILGUN_API_KEY),
        data={
            "from": "No Reply <noreply@satdat.space>",
            "to": [user_email],
            "subject": mail_subject,
            "html": mail_text,
        },
    )

    module_logger.info(result)

    module_logger.info(job.id)

    module_logger.info(f"Completed {job_type}")
    module_logger.info("Sending email using mailgun")


@app.task(bind=True)
def l8_batch_download(self, order_id, working_directory):
    downloader = L8Downloader(path_to_config=settings.CONFIG_PATH, verbose=False)
    working_directory = Path(working_directory)

    self.update_state(
        state=states.STARTED,
        meta={
            "download": "in progress",
            "extract": "not started",
            "upload": "not started",
        },
    )

    download_result = downloader.download_order(order_id, directory=working_directory)

    # if the download is successful, for each file, extract and upload to s3 bucket
    if download_result.status:
        module_logger.info(
            "Overall download succesful, now extracting and uploading individual products."
        )
        self.update_state(
            state=states.STARTED,
            meta={
                "download": "finished",
                "extract": "not started",
                "upload": "not started",
            },
        )

        for file_name in download_result.data:
            file_path = Path(working_directory, file_name)

            self.update_state(
                state=states.STARTED,
                meta={
                    "download": "finished",
                    "extract": "in progress",
                    "upload": "not started",
                },
            )

            extract_result = unarchive(file_path, working_directory)

            if extract_result.status:
                module_logger.info(f"Extraction for file {file_name} was successful.")
                self.update_state(
                    state=states.STARTED,
                    meta={
                        "download": "finished",
                        "extract": "finished",
                        "upload": "in progress",
                    },
                )
                upload_result = upload_l8_l2a_to_s3(extract_result.message)

                if upload_result.status:
                    module_logger.info(f"Upload for file {file_name} was successful.")
                    self.update_state(
                        state=states.SUCCESS,
                        meta={
                            "download": "finished",
                            "extract": "finished",
                            "upload": "finished",
                        },
                    )

                    # Download, extract and Upload successful, need to upate the individual jobs for each tile
                    clean_up_folder(working_directory)

                else:
                    raise TaskFailureException(f"Upload failed for file {file_name}")
            else:
                raise TaskFailureException(f"Extraction failed for file {file_name}")
    else:
        raise TaskFailureException("Download failed.")


# @app.task(ignore_result=True)
# def check_l8_batch_jobs():
#     """Periodic task specically for L8 Batch Jobs"""

#     from .models import Job
#     from .models import JobStatusChoice
#     from .models import JobPriorityChoice

#     jobs = (
#         Job.objects.select_for_update()
#         .filter(
#             (
#                 Q(job_type__exact="S2Download")
#                 | Q(job_type__exact="L8Download")
#                 | Q(job_type__exact="Sen2AgriL2A")
#             )
#             & (
#                 Q(status__exact=JobStatusChoice.SUBMITTED.value)
#                 | Q(status__exact=JobStatusChoice.ASSIGNED.value)
#             )
#         )
#         .order_by("submitted")
#     )

#     with transaction.atomic():
#         module_logger.info(jobs)
#         for job in jobs:

#             if job.job_type in ["S2Download"]:
#                 module_logger.info(job.id)
#                 module_logger.info(job.task_id)
#                 res = AsyncResult(job.task_id)

#                 task_state = res.state
#                 module_logger.info(task_state)
#                 now_datetime = datetime.now()

#                 if task_state == states.STARTED:

#                     job.status = JobStatusChoice.ASSIGNED.value
#                     job.assigned = now_datetime

#                 elif task_state == states.SUCCESS or task_state == states.FAILURE:

#                     if job.status == JobStatusChoice.SUBMITTED.value:
#                         job.assigned = now_datetime

#                     job.status = JobStatusChoice.COMPLETED.value
#                     job.completed = now_datetime
#                     job.success = True if task_state == states.SUCCESS else False

#                 job.save()

#             elif job.job_type in ["L8Download"] and job.parameters["options"]["ac"]:
#                 module_logger.info(job.id)
#                 module_logger.info(job.task_id)
#                 res = AsyncResult(job.task_id)

#                 task_state = res.state
#                 module_logger.info(task_state)
#                 now_datetime = datetime.now()

#                 if task_state == states.STARTED:

#                     job.status = JobStatusChoice.ASSIGNED.value
#                     job.assigned = now_datetime

#                 elif task_state == states.SUCCESS or task_state == states.FAILURE:

#                     if job.status == JobStatusChoice.SUBMITTED.value:
#                         job.assigned = now_datetime

#                     job.status = JobStatusChoice.COMPLETED.value
#                     job.completed = now_datetime
#                     job.success = True if task_state == states.SUCCESS else False

#                 job.save()

#             elif job.job_type in ["Sen2Agri_L2A"]:
#                 # Steps:
#                 # check meta data for each associated task in the task_id list
#                 # update the task progress for each task id in the list
#                 # if all tasks in the list have status of finished, update the job state to completed.
#                 module_logger.info("This is a Sen2Agri_L2A job")

#             elif job.job_type in ["L8BatchJob"]:

#                 module_logger.info("This is a L8BatchJob")

#                 if job.status == JobStatusChoice.SUBMITTED.value:
#                     downloader = L8Downloader(
#                         path_to_config=settings.CONFIG_PATH, verbose=False
#                     )

#                     # Check order status
#                     order_id = job.parameters["usgs_order_id"]
#                     module_logger.debug(order_id)
#                     order_ready = downloader.check_order_status(order_id)
#                     module_logger.debug(order_ready)

#                     if str(job.task_id) != "00000000-0000-0000-0000-000000000000":
#                         res = AsyncResult(job.task_id)
#                         task_state = res.state
#                         now_datetime = datetime.now()
#                         module_logger.info(res.info)

#                         if task_state == states.STARTED:

#                             job.status = JobStatusChoice.ASSIGNED.value
#                             job.assigned = now_datetime
#                             new_info = job.info.copy()
#                             module_logger.info(res.info)

#                             new_info["task_progress"] = res.info
#                             job.info = new_info

#                         elif (
#                             task_state == states.SUCCESS or task_state == states.FAILURE
#                         ):

#                             if job.status == JobStatusChoice.SUBMITTED.value:
#                                 job.assigned = now_datetime

#                             job.status = JobStatusChoice.COMPLETED.value
#                             new_info = job.info.copy()
#                             module_logger.info(res.info)

#                             new_info["task_progress"] = res.info

#                             job.completed = now_datetime
#                             job.success = (
#                                 True if task_state == states.SUCCESS else False
#                             )
#                     else:
#                         if order_ready:
#                             task_id = l8_batch_download.delay(
#                                 order_id, str(WORKING_FOLDER_PATH)
#                             )
#                             job.info = {"order_status": "ready"}
#                             job.task_id = str(task_id)
#                         else:
#                             module_logger.info(f"Order {order_id} not ready yet.")
#                             job.info = {"order_status": "not ready"}

#                 elif job.status == JobStatusChoice.ASSIGNED.value:
#                     res = AsyncResult(job.task_id)

#                     task_state = res.state
#                     now_datetime = datetime.now()

#                     if task_state == states.STARTED:

#                         job.status = JobStatusChoice.ASSIGNED.value
#                         job.assigned = now_datetime
#                         job.info = job.info["task_progress"] = res.info

#                     elif task_state == states.SUCCESS or task_state == states.FAILURE:

#                         if job.status == JobStatusChoice.SUBMITTED.value:
#                             job.assigned = now_datetime

#                         job.status = JobStatusChoice.COMPLETED.value
#                         job.info = job.info["task_progress"] = res.info
#                         job.completed = now_datetime
#                         job.success = True if task_state == states.SUCCESS else False


#                 job.save()

@app.task
def bulk_fetch_l8():
    """Periodic task to check the status of USGS job, and download if ready

        Steps:
        1. Fetch one jobs of type L8BatchJob, status submitted, with the oldest submitted date
        2. Check the job status
        3. If the job is ready, change job status to assigned
        4. If the job is ready, download the products, extract, and upload to s3
        5. For each associated job id for each tile, when the upload completes, mark the job as complete
        6. When all sub jobs are complete, mark the overall job as complete

    """
    from .models import Job
    from .models import JobStatusChoice
    from .models import JobPriorityChoice

    downloader = L8Downloader(path_to_config=settings.CONFIG_PATH, verbose=False)

    module_logger.debug(JobStatusChoice.SUBMITTED)
    jobs = (
        Job.objects.select_for_update()
        .filter(
            Q(job_type__exact="L8BatchJob")
            & Q(status__exact=JobStatusChoice.SUBMITTED.value)
        )
        .order_by("-submitted")
    )
    with transaction.atomic():
        module_logger.debug(jobs)
        job = None

        if len(jobs) > 0:
            job = jobs[0]

        if job:
            order_id = job.parameters["usgs_order_id"]
            tile_list = job.parameters["tile_list"]

            order_ready = downloader.check_order_status(order_id)

            if order_ready:
                now_datetime = datetime.now()
                job.status = JobStatusChoice.ASSIGNED.value
                job.assigned = now_datetime
                job.save()

                download_folder_path = Path(settings.BASE_DIR, "working_folder")
                try:
                    download_result = downloader.download_order(
                        order_id, directory=download_folder_path
                    )
                except BaseException as e:
                    module_logger.debug("something went wrong while trying to download")
                    download_result = TaskStatus(False, "Download step failed", str(e))

                # if the download is successful, for each file, extract and upload to s3 bucket
                if download_result[0]:

                    task_status_list = []
                    for file_name in download_result[2]:
                        file_path = Path(WORKING_FOLDER_PATH, file_name)
                        extract_result = unarchive(file_path, WORKING_FOLDER_PATH)

                        if extract_result[0]:
                            upload_result = upload_l8_l2a_to_s3(extract_result[1])
                        else:
                            job.status = JobStatusChoice.SUBMITTED.value
                            job.assigned = None
                            job.save()

                            return TaskStatus(False, "Extract step failed", None)

                        if upload_result[0]:
                            # Download, extract and Upload successful, need to upate the individual jobs for each tile

                            task_status_list.append(
                                TaskStatus(True, extract_result[1], None)
                            )
                        else:
                            job.status = JobStatusChoice.SUBMITTED.value
                            job.assigned = None
                            result_message = "Upload step failed"
                            job.result_message = result_message
                            job.save()

                            clean_up_folder(WORKING_FOLDER_PATH)

                            return TaskStatus(False, result_message, None)
                    else:
                        now_datetime = datetime.now()
                        overall_task_result = True

                        for task_status in task_status_list:

                            module_logger.debug(task_status)
                            module_logger.debug(tile_list)

                            if task_status[0]:

                                job_id = [
                                    t["job_id"]
                                    for t in tile_list
                                    if t["name"] == task_status[1]
                                ][0]
                                tile_job = Job.objects.get(id=job_id)
                                tile_job.status = JobStatusChoice.COMPLETED.value
                                tile_job.success = True
                                tile_job.completed = now_datetime
                                tile_job.save()
                            else:
                                job_id = [
                                    t["job_id"]
                                    for t in tile_list
                                    if t["name"] == task_status[1]
                                ][0]
                                tile_job = Job.objects.get(id=job_id)
                                tile_job.status = JobStatusChoice.COMPLETED.value
                                tile_job.success = False
                                tile_job.completed = now_datetime
                                tile_job.save()
                                overall_task_result = False

                        job.status = JobStatusChoice.COMPLETED.value

                        job.completed = now_datetime
                        if overall_task_result:
                            job.success = True
                            result_message = "Download, Extract, and Upload successful"
                        else:
                            job.success = False

                            result_message = "Some part of the job failed"

                        job.result_message = result_message
                        job.save()

                        clean_up_folder(WORKING_FOLDER_PATH)

                else:
                    # download process failed, abort thejob, revert the changes to the job
                    job.status = JobStatusChoice.SUBMITTED.value
                    job.assigned = None
                    result_message = "The order is ready, but the download step failed."
                    job.result_message = result_message

                    job.save()

                    return TaskStatus(False, result_message, None)

            else:
                return TaskStatus(False, f"Order is not ready : {order_id}", None)
        else:
            return TaskStatus(True, "No L8Batch job types found, no work to do.", None)


def update_job_existing_l2a(job, job_status, job_success, result_message):
    now_datetime = datetime.now()
    job.assigned = now_datetime
    job.completed = now_datetime
    job.status = job_status
    job.success = job_success
    job.result_message = result_message

    job.save()


@app.task
def bulk_submit_l8():
    """Periodic task to check for L8 requests, bundle and submit to USGS bulk service
    
    Steps:
        1. Fetch all jobs of type L8BulkDownload, status submitted
        2. Get tile name from each job
        3. Submit request to USGS in groups of 10 tiles
        4. Add job of type L8Batch to track each USGS batch:
            L8Batch job needs, USGS job id, tile name, and job id for each tile

    """

    from .models import Job
    from .models import JobStatusChoice
    from .models import JobPriorityChoice

    module_logger.debug(JobStatusChoice.SUBMITTED)
    jobs = Job.objects.select_for_update().filter(
        Q(job_type__exact="L8Download")
        & Q(status__exact=JobStatusChoice.SUBMITTED.value)
    )

    with transaction.atomic():
        module_logger.debug(jobs)

        tile_tuples = []
        for job in jobs:
            module_logger.debug(job.status)
            module_logger.debug(job.parameters["options"])
            params = job.parameters["options"]
            if params["ac"]:
                tile = params["tile"]
                job_id = str(job.id)

                l2a_exists = check_if_l8_l2a_imagery_exists(tile)
                if not l2a_exists[0]:
                    tile_tuples.append({"name": tile, "job_id": job_id})
                else:
                    update_job_existing_l2a(
                        job,
                        JobStatusChoice.COMPLETED.value,
                        True,
                        "L2A product already exists.",
                    )

        module_logger.debug(tile_tuples)

        batch_dict = None
        if len(tile_tuples) > 0:
            # Submit tiles to the USGS here
            batch_dict = bulk_download_l8_submit(tile_tuples)

        if batch_dict:

            module_logger.debug(batch_dict)

            batch_id_list = []

            for job in jobs:
                for key, value in batch_dict.items():
                    module_logger.debug(key)
                    module_logger.debug(value)
                    param_dict = {"usgs_order_id": key, "tile_list": value}

                    for t in value:
                        module_logger.debug(t)
                        if str(job.id) == t["job_id"]:
                            now_datetime = datetime.now()
                            job.assigned = now_datetime
                            job.status = JobStatusChoice.ASSIGNED.value
                            job.save()

            batch_job = Job(
                job_type="L8BatchJob",
                label="L8BatchJob",
                command="na",
                parameters=param_dict,
                priority=JobPriorityChoice.LOW.value,
            )
            batch_job.owner_id = 1
            batch_job.save()

            batch_id_list.append(str(batch_job.id))

            return batch_id_list
        else:
            return str("No jobs to submit.")


def bulk_download_l8_submit(tuple_list):
    """ Helper function to submit jobs to the USGS Bulk Download Service

    Should return a dictionary  of order ids that each have a list of tiles associated with it
    """
    batch_size = 10
    downloader = L8Downloader(path_to_config=settings.CONFIG_PATH, verbose=False)

    if len(tuple_list) == 0:
        return None

    batch_list = []

    # Parse the entire list into a series of slices the size of the batch
    for idx in range(0, (len(tuple_list) // 10) + 1):
        batch_list.append(
            tuple_list[idx * batch_size : (idx * batch_size) + batch_size]
        )

    module_logger.info(len(tuple_list))

    batch_dict = {}
    for batch in batch_list:
        res = downloader.bulk_submit_order(batch)
        module_logger.debug(res)
        if res:
            batch_dict[res] = batch

        # Avoid USGS rate limiting
        time.sleep(30)

    module_logger.info(batch_dict)

    if len(batch_dict.keys()) == 0:
        return None
    else:
        return batch_dict


def upload_l8_l2a_to_s3(tile_name):
    logging.info(tile_name)

    name_split = tile_name.split("_")
    path = name_split[2][:3]
    row = name_split[2][3:]

    for file_name in os.listdir(WORKING_FOLDER_PATH):

        if file_name.startswith(tile_name):
            upload_success = s3_helper.upload_file_s3(
                Path(WORKING_FOLDER_PATH, file_name),
                str(Path(path, row, tile_name)),
                OUTPUT_BUCKET_NAME,
            )

            if not upload_success[0]:
                return TaskStatus(False, "Upload of L2A Failed", None)

    return TaskStatus(True, "Upload of L2A Succeeded", None)


def check_if_l8_l2a_imagery_exists(l1c_name):
    logging.info("checking if imagery exists on s3")
    logging.info(l1c_name)

    # /mnt/drobos/zeus/minio_storage/l8-l2a-products/039/023/LC08_L1TP_039023_20190601_20190605_01_T1

    l1c_split = l1c_name.split("_")
    path = l1c_split[2][:3]
    row = l1c_split[2][3:]

    search_expression = r"LC08_L1[TG][PTS]_{}_{}_\d{{8}}_01_T[12]".format(
        l1c_split[2], l1c_split[3]
    )
    logging.info("checking if object exists in s3 with wildcards")

    # search_expresssion, object_prefix, bucket_name
    object_exists = s3_helper.check_object_exists_in_s3_wildcards(
        search_expression, "/".join([path, row]), OUTPUT_BUCKET_NAME
    )

    return object_exists


@app.task
def start_s2_batch_job(imagery_list, ac, ac_res, user_id):
    """Handles initial creation of the S2 Batch job.

    For each image in the list:
    If image is "online":
        dispatch s2 download task
    else:
        add image to redis offline request queue

    return task Ids and redis keys for each image
    """
    # TODO Should move the email sending to when the job is saved, or actually started, so a
    # job id can be sent to the submitter
    # def send_simple_message():
    #     return requests.post(
    #         "https://api.mailgun.net/v3/mg.satdat.space/messages",
    #         auth=("api", "api key goes here"),
    #         data={
    #             "from": "No Reply <noreply@satdat.space>",
    #             "to": ["shauncullen@gmail.com"],
    #             "subject": "New S2 Batch Job Received",
    #             "text": "Great news! Your S2 Batch Job has been recieved and is under way. You will receive another email when the job is complete!",
    #         },
    #     )

    module_logger.info("S2 Batch Job creation task starting...")

    module_logger.info(user_id)

    module_logger.info(imagery_list)
    module_logger.info(ac)
    module_logger.info(ac_res)

    job_start_ts = int(time.time())
    task_id_dict = {}

    s2_downloader = S2Downloader(path_to_config=settings.CONFIG_PATH)

    for product in imagery_list:
        module_logger.info(product)
        product_id = product["entity_id"]

        product_info = s2_downloader.get_product_info(product_id)
        product_online = product_info["Online"]

        if product_online:
            module_logger.info("Product online, proceeding to download right away")
            # result_list = start_job(
            # params["tile"],
            # atmospheric_correction=params["ac"],
            # ac_res=params["ac_res"],
            # api_source=params["api_source"],
            # entity_id=params["entity_id"],
            # celery_task=self,
            params = {
                "options": {
                    "tile": product["name"],
                    "ac": ac,
                    "ac_res": ac_res,
                    "api_source": product["api"],
                    "entity_id": product["entity_id"],
                }
            }

            task_id = download_s2.delay(params)
            task_id_dict[str(task_id)] = product["tile_id"]
        else:
            module_logger.info(
                "Product offline, waiting until product is online before downloading"
            )
            # add to redis queue for user

            # keys for items in the queue should be user:s2:tileid
            # each item in the queue is a hashmap with date, tile id, product name, etc
            # the actual queue is a zset, with hmkey and z value of the timestamp

            # there should be a key for the active item, user:s2:active pointing to one of the hmkeys

            # The active item should be set from the zset, using timestamp as the ranking (smallest timestamp should be selected)
            # when a offline item is ready, a download s2 task is submited and

            key = f'{user_id}:s2:{product["tile_id"]}'

            value = {
                "tile": product["name"],
                "ac": int(ac),
                "ac_res": ",".join([str(x) for x in ac_res]),
                "api_source": product["api"],
                "entity_id": product["entity_id"],
            }

            redis_instance.hmset(key, value)

            task_id_dict["redis" + key] = product["tile_id"]

            redis_instance.zadd(f"{user_id}:s2:queue", {key: job_start_ts})

    return task_id_dict


@app.task
def start_l8_batch_job(imagery_list, ac, ac_res, user_id):
    """Handles initial creation of the S2 Batch job.

    If ac is false:
    create an L8 download job for each tile in the imagery list, and save the task ids in the info property of the job

    If ac is true:
    for each batch of 10 tiles, create an L8 bulk download job, store the task ids in the info property

    """

    module_logger.info("L8 Batch Download creation task starting...")

    module_logger.info(user_id)

    module_logger.info(imagery_list)
    module_logger.info(ac)
    module_logger.info(ac_res)

    # check for ac correction here, if ac is present, mimic l8 batch job code, else proceed as normal
    imagery_to_submit = []
    imagery_already_exists = []

    if ac:
        module_logger.info("Using bulk download service, submitting orders for requested tiles")
        for imagery in imagery_list:
            exists_on_s3 = check_if_l8_l2a_imagery_exists(imagery["name"])
            module_logger.info(f'Exists on S3: {exists_on_s3}')
            if not exists_on_s3.status:
                imagery_to_submit.append({"name": imagery["name"]})
            else:
                imagery_already_exists.append({"name": imagery["name"]})
        try:
            order_id_dict = bulk_download_l8_submit(imagery_to_submit)
            order_id_dict['imagery_exists_on_s3'] = imagery_already_exists
        except BaseException as e:
            module_logger.error(e)
            raise TaskFailureException('Failure during task creation')

        module_logger.info(order_id_dict)

        if not order_id_dict or len(order_id_dict.keys()) == 1:
            raise TaskFailureException('Failure during task creation')

        return order_id_dict
    else:
        task_id_dict = {}

        for product in imagery_list:
            module_logger.info(product)

            module_logger.info("Not using bulk download service, proceeding to download right away")
            # result_list = start_job(
            # params["tile"],
            # atmospheric_correction=params["ac"],
            # ac_res=params["ac_res"],
            # api_source=params["api_source"],
            # entity_id=params["entity_id"],
            # celery_task=self,
            params = {
                "options": {
                    "tile": product["name"],
                    "ac": ac,
                    "ac_res": ac_res,
                    "api_source": product["api"],
                    "entity_id": product["entity_id"],
                }
            }

            task_id = download_l8.delay(params)
            task_id_dict[str(task_id)] = product["tile_id"]


        return task_id_dict

