"""
simple_worker.py

1. request job from /job endpoint (JobManager API)
2. run job, including command and parameters
3. upload final processed results to S3 store
4. report job as succeeded or failure

"""
import argparse
import asyncio
import json
from pathlib import Path
import os
import shutil
import subprocess
import sys
import time
import zipfile

import multiprocessing
from subprocess import Popen, PIPE

import boto3
from botocore.client import Config
import botocore

import requests as req
from requests.auth import HTTPBasicAuth

import logging
import yaml

from django.conf import settings

# set up logging to file - see previous section for more details
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    datefmt="%m-%d %H:%M",
    filename="simpleworker.log",
    filemode="w",
)

# define a Handler which writes INFO messages or higher to the sys.stderr
console = logging.StreamHandler()
console.setLevel(logging.INFO)
# set a format which is simpler for console use
formatter = logging.Formatter("%(name)-12s: %(levelname)-8s %(message)s")
# tell the handler to use this format
console.setFormatter(formatter)
# add the handler to the root logger
logging.getLogger("").addHandler(console)

from common import system_info
from common.utils import ConfigValueMissing, ConfigFileProblem

from worker.tasks import sen2agri_tasks


def get_system_info():
    si = system_info.SystemInfo()
    print(si.get_info_dict())


BASE_DIR = settings.BASE_DIR
CONFIG_PATH = Path(BASE_DIR, "worker_config.yaml")

# Load config from config.yaml
try:
    with open(CONFIG_PATH, "r") as stream:
        config = yaml.safe_load(stream)

except yaml.YAMLError as exc:
    logging.error("Problem loading config... exiting...")
    raise ConfigFileProblem

# JOB_MANAGER_API_URL: http://hal678772.agr.gc.ca:30199/
# WORKER_NAME: ubuntu_worker_20191028
# CAPABILITIES:
#   - S2Download
#   - L8Download
# JOB_MANAGER_API_PASS: aafc

required_config_keys = [
    "JOB_MANAGER_API_URL",
    "WORKER_NAME",
    "CAPABILITIES",
    "JOB_MANAGER_API_PASS",
]

for key in config.keys():
    if key not in required_config_keys:
        raise ConfigValueMissing

# S3 Config for S3Utils
SERVER_URL = config["JOB_MANAGER_API_URL"]

MAX_S3_ATTEMPTS = 10


globals().update(locals())


def parse_args():
    parser = argparse.ArgumentParser(
        description="Basic worker that queries for tasks and executes them."
    )

    # parser.add_argument('-n',
    #                     metavar='worker_name', dest="worker_name", action='store',
    #                     type=str,
    #                     help='Name for the worker.',
    #                     required=True)

    # parser.add_argument('-s',
    #                     metavar='server_name', dest="server_name", action='store',
    #                     type=str,
    #                     help='Server host name and port (localhost:8000 for example).',
    #                     required=False,
    #                     default='127.0.0.1:8000')

    args_obj = parser.parse_args()

    return args_obj


def request_job(worker_id):
    # request a job

    payload = {"worker_id": worker_id}

    r = req.get(f"{SERVER_URL}/job", params=payload)

    job_dict = json.loads(r.text)

    return job_dict


def start_job(worker_id, job_dict):

    # asyncio.set_event_loop(None)
    # loop = asyncio.new_event_loop()
    # asyncio.get_child_watcher().attach_loop(loop)
    job_success = False
    print("hello")
    print(job_dict)
    print("starting job")

    if job_dict["job_type"].startswith("Sen2Agri_L2A"):
        params = job_dict["parameters"]

        imagery_list = params["imagery_list"]
        aoi_name = params["aoi_name"]
        window_size = params["window_size"]

        job_id = job_dict["id"]

        result_list = sen2agri_tasks.start_l2a_job(
            imagery_list, job_id, aoi_name, window_size
        )
        logging.info(result_list)
        overall_result = True
        result_strings = []
        logging.info("Job is finished, reporting to the server")
        for res in result_list:
            logging.info(res)
            tile, res_date_dict = next(iter(res.items()))
            logging.info(tile)
            logging.info(res_date_dict)
            for key_date, value in res_date_dict.items():
                result_string = f"For {tile}:{key_date} = {value[0]}: {'Success' if value[0] else 'Failed'}, msg: {value[1]}"
                result_strings.append(result_string)

                if not value[0]:
                    overall_result = False

        result_message = "\n".join(result_strings)

        report_job_to_server(overall_result, result_message, worker_id, job_id)


def report_job_to_server(job_status, result_message, worker_id, job_id):
    current_attempt = 0
    attempt_success = False

    while current_attempt < MAX_S3_ATTEMPTS:
        print("Trying to update job status on Job Manager server...")
        try:
            r = req.post(
                f"{SERVER_URL}/jobs/{job_id}/",
                json={
                    "status": "C",
                    "worker_id": worker_id,
                    "result_message": result_message,
                    "success": job_status,
                },
                verify=False,
            )
        except Exception as e:
            print(e)
            print("Failure occurred when trying to update job status")
            current_attempt += 1
            print(f"Trying again ({current_attempt}/{MAX_S3_ATTEMPTS})")

        else:
            attempt_success = True
            break
            print(r)
            if r.status_code == 200:
                print("job updated on server successfully!")
            else:
                print("something went wrong trying to update job status")

    return attempt_success


# now that the API is working correctly, registration is the first thing required
# register with the job manager server
#  'worker_name': 'test_worker',
#         'system_info': {
#             'hostname': 'OTT-UBU-01',
#             'system_platform': 'linux',
#             'platform': 'Linux-4.15.0-45-generic-x86_64-with-Ubuntu-18.04-bionic',
#             'cpu': 'x86_64',
#             'os': 'Linux',
#             'os_ver': '#48-Ubuntu SMP Tue Jan 29 16:28:13 UTC 2019',
#             'os_rel': '4.15.0-45-generic',
#             'cores': 4
#         },
#         'capabilities': [
#             'S1_Preprocessing_v1', 'S2_Atmoscor_v1'
#         ],
#         'server_code': 'aafc'

sys_info = system_info.SystemInfo().get_info_dict()

worker_data = {
    "worker_name": config["WORKER_NAME"],
    "system_info": sys_info,
    "capabilities": config["CAPABILITIES"],
    "server_code": config["JOB_MANAGER_API_PASS"],
}

logging.info(SERVER_URL)

globals().update(locals())

try:
    print("Trying to register worker with the Job Management server")
    logging.info(f"{SERVER_URL}/worker/")
    r = req.post(f"{SERVER_URL}/worker/", json=worker_data)
except BaseException as e:
    print("Failure occured when trying to register with the job management server.")
    print("Restart the worker and try again")
    print(e)
else:
    print(r)

    response_data = None
    worker_id = None

    if r.status_code in [200, 201]:
        print("worker registered successfully")
        print(r.json())
        response_data = r.json()
        worker_id = response_data["worker_id"]
    else:
        print("worker registration failed")
        raise BaseException("Worker registrion failed")

    while True:
        job = request_job(worker_id)

        print(job)

        if "info" in job.keys() and job["info"].lower().startswith(
            "no jobs currently available"
        ):
            print("no jobs available, checking in 10 seconds")

        else:
            print("Job Available, starting....")
            start_job(worker_id, job)

        time.sleep(10)
