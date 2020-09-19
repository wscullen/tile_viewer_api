import boto3
from botocore.client import Config
from boto3.s3.transfer import TransferConfig
import botocore
import json
import logging
from pathlib import Path
import os
import re
import time

from common.utils import (
    TaskStatus,
    get_partial_path,
    ConfigFileProblem,
    ConfigValueMissing,
)

import os
import sys
import threading

from celery import states, app, current_task
from celery.result import AsyncResult


class ProgressPercentage(object):
    def __init__(self, filename, celery_task, celery_task_id=None, order_id=None):
        self.logger = logging.getLogger("common.s3_utils.ProgressPercentage")
        
        self.logger.info(filename)

        self.path_to_be_uploaded = Path(filename)

        if self.path_to_be_uploaded.is_dir():
            self.size = sum(f.stat().st_size for f in self.path_to_be_uploaded.glob('**/*') if f.is_file())
        else:
            self.filename = filename
            self.size = float(self.path_to_be_uploaded.stat().st_size)
        
        if order_id:
            self.order_id = order_id

        self.celery_task = celery_task
        self.celery_task_id = celery_task_id
        self.seen_so_far = 0
        self.previous_update = 0
        self.update_throttle_threshold = 1
        self.lock = threading.Lock()

    def __call__(self, bytes_amount):

        with self.lock:
            self.seen_so_far += bytes_amount
        
        self.progress_percent = round(
                min(100, (self.seen_so_far / self.size) * 100), 2
            )

        if (
            self.progress_percent - self.previous_update
        ) > self.update_throttle_threshold:
            self.logger.info(self.progress_percent)
            try:
                self.logger.info(self.celery_task)
                self.logger.info(self.celery_task_id)
                task_result = AsyncResult(self.celery_task_id)
                
                self.logger.info(task_result)
                self.logger.info(task_result.info)
                self.logger.info(task_result.state)
                
                task_info = task_result.info.copy()
                
                if hasattr(self, "order_id"):
                    task_info[self.path_to_be_uploaded.stem]["upload"] = (
                        self.progress_percent if self.progress_percent < 99.0 else 100.0
                    )
                else:
                    task_info["upload"] = (
                        self.progress_percent if self.progress_percent < 99.0 else 100.0
                    )

                self.logger.info(task_info)

                self.celery_task.update_state(
                    state=states.STARTED,
                    meta=task_info,
                )

            except BaseException as e:
                self.logger.info(str(e))

            self.previous_update = self.progress_percent


import aiobotocore


class S3UtilityAio:
    """ Same as S3 utility, only using the aiobotocore library to be
        Asyncio compat.
    """

    def __init__(self, config_dict, working_folder, max_attempts=20, time_to_wait=60):

        self.logger = logging.getLogger("common.s3_utils.S3UtilityAio")
        self.MAX_ATTEMPTS = max_attempts
        self.WAIT_TIME = time_to_wait

        required_config_keys = ["S3_URL", "S3_ACCESS_KEY", "S3_SECRET_KEY", "S3_REGION"]
        for key in config_dict.keys():
            if key not in required_config_keys:
                raise ConfigValueMissing

        self.ENDPOINT_URL = config_dict["S3_URL"]  # internal GoC url for minio server
        self.ACCESS_KEY = config_dict["S3_ACCESS_KEY"]
        self.SECRET_KEY = config_dict["S3_SECRET_KEY"]
        self.CONFIG = Config(
            signature_version="s3v4"
        )  # Will have to see if this config version works with DO Spaces API
        self.REGION = config_dict[
            "S3_REGION"
        ]  # 'us-east-1' for GoC interal minio server
        print("hello")
        self.WORKING_FOLDER_PATH = working_folder

    async def iterate_bucket_items(self, bucket, prefix):
        session = aiobotocore.get_session()

        async with session.create_client(
            "s3",
            region_name=self.REGION,
            aws_secret_access_key=self.SECRET_KEY,
            aws_access_key_id=self.ACCESS_KEY,
            endpoint_url=self.ENDPOINT_URL,
        ) as client:

            # list s3 objects using paginator
            paginator = client.get_paginator("list_objects")
            async for result in paginator.paginate(Bucket=bucket, Prefix=prefix):
                if "Contents" in result.keys():
                    # self.logger.warning("inside paginator")
                    for item in result["Contents"]:
                        # self.logger.warning(item)
                        yield item

    async def check_object_exists_in_s3_wildcards(
        self,
        search_expression,
        object_prefix,
        bucket_name,
        image_name=None,
        api_source=None,
    ):
        """ Allows the use of regular expressions in the search_expression
            Search for existence without exact name
        """
        logging.info("checking s3 with wild cards")

        current_attempt = 0

        object_exists = False

        self.logger.info(f"Bucket: {bucket_name} -- Prefix: {object_prefix}")

        while current_attempt < self.MAX_ATTEMPTS:
            try:
                async for item in self.iterate_bucket_items(bucket_name, object_prefix):
                    # logging.info(item)
                    if object_prefix == "":
                        suffix = item["Key"]
                    else:
                        suffix = item["Key"].split("/")[-2]
                    # logging.info(suffix)
                    search_result = re.search(search_expression, suffix)
                    # logging.info(item)
                    if search_result:
                        logging.info("match found")
                        logging.info(search_result.group(0))
                        object_exists = search_result.group(0)
                        logging.info(item)
                        logging.info(object_exists)
                        break

                else:
                    logging.info("no match found")
                    if image_name:
                        if api_source:
                            return {image_name: {api_source: "object does not exist"}}
                        else:
                            return {image_name: "object does not exist"}
                    else:
                        return TaskStatus(False, "object does not exist", None)

            except botocore.exceptions.ClientError as e:
                logging.info(
                    f"encountered an exception when accessing the s3 server: {e}"
                )
                if e.response["Error"]["Code"] == "404":
                    logging.info("object does not exist")
                    if image_name:
                        if api_source:
                            return {image_name: {api_source: "object does not exist"}}
                        else:
                            return {image_name: "object does not exist"}
                    else:
                        return TaskStatus(False, "object does not exist", e)
                else:
                    logging.info("Unable to determine if tile exists in local S3 repo")
                    logging.info(f"Trying again ({current_attempt}/{self.MAX_ATTEMPTS}")
                    current_attempt += 1
                    if current_attempt == self.MAX_ATTEMPTS:
                        if image_name:
                            if api_source:
                                return {
                                    image_name: {
                                        api_source: "unable to communicate with S3 bucket"
                                    }
                                }
                            else:
                                return {
                                    image_name: "unable to communicate with S3 bucket"
                                }
                        else:
                            return TaskStatus(
                                False, "unable to communicate with S3 bucket", None
                            )
            else:
                if image_name:
                    if api_source:
                        return {image_name: {api_source: object_exists}}
                    else:
                        return {image_name: object_exists}
                else:
                    return TaskStatus(True, object_exists, None)


class S3Utility:
    def __init__(self, config_dict, working_folder, max_attempts=5, time_to_wait=60):

        self.logger = logging.getLogger("common.s3_utils.S3Utility")

        self.MAX_S3_ATTEMPTS = max_attempts
        self.WAIT_TIME = time_to_wait

        required_config_keys = ["S3_URL", "S3_ACCESS_KEY", "S3_SECRET_KEY", "S3_REGION"]

        for key in config_dict.keys():
            if key not in required_config_keys:
                raise ConfigValueMissing

        self.ENDPOINT_URL = config_dict["S3_URL"]  # internal GoC url for minio server
        self.ACCESS_KEY = config_dict["S3_ACCESS_KEY"]
        self.SECRET_KEY = config_dict["S3_SECRET_KEY"]
        self.CONFIG = Config(
            signature_version="s3v4"
        )  # Will have to see if this config version works with DO Spaces API
        self.REGION = config_dict[
            "S3_REGION"
        ]  # 'us-east-1' for GoC interal minio server

        self.WORKING_FOLDER_PATH = working_folder

        try:
            self.s3_resource = boto3.resource(
                "s3",
                endpoint_url=self.ENDPOINT_URL,
                aws_access_key_id=self.ACCESS_KEY,
                aws_secret_access_key=self.SECRET_KEY,
                config=Config(signature_version="s3v4"),
                region_name=self.REGION,
            )
        except:
            self.logger.error("failed to connect to s3 service")
            raise S3ResourceConnectionProblem

        try:
            self.s3_client = boto3.client(
                "s3",
                endpoint_url=self.ENDPOINT_URL,
                aws_access_key_id=self.ACCESS_KEY,
                aws_secret_access_key=self.SECRET_KEY,
                config=Config(signature_version="s3v4"),
                region_name=self.REGION,
            )
        except:
            self.logger.error("failed to connect to s3 service")
            raise S3ClientConnectionProblem

    def delete_bucket(self, bucket_name):
        try:
            self.s3_resource.Bucket(bucket_name).delete()

        except BaseException as e:
            self.logger.info("Exception encountered:")
            self.logger.info(e)
            self.logger.info("Bucket deletion failed")

        else:
            self.logger.info("Bucket deletion succeeded!")

    def iterate_bucket_items(self, bucket, prefix):
        """
        Generator that iterates over all objects in a given s3 bucket

        See http://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Client.list_objects_v2
        for return data format
        :param bucket: name of s3 bucket
        :return: dict of metadata for an object
        """

        paginator = self.s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix, PaginationConfig={"PageSize": 100})
        self.logger.debug(page_iterator)
        print(page_iterator)
        count = 0
        pages_with_content = 0
        try:
            for page in page_iterator:
                self.logger.debug(page)
                # print(page)
                
                count += 1
                if "Contents" in page.keys():
                    print(len(page["Contents"]))
                    pages_with_content += 1
                    print(f'_____\n\n\n\n {count} \n\n\n\n\n')
                    print(pages_with_content)

                    for item in page["Contents"]:
                        yield item
                else:
                    continue
        except BaseException as e:
            print(e)
        
    def check_object_exists_in_s3_wildcards(
        self, search_expression, object_prefix, bucket_name
    ):
        """ Allows the use of regular expressions in the search_expression
            Search for existence without exact name
        """
        self.logger.info("checking s3 with wild cards")

        current_attempt = 0
        time_to_wait = self.WAIT_TIME

        object_exists = False

        self.logger.info(f"Bucket: {bucket_name} -- Prefix: {object_prefix}")

        while current_attempt < self.MAX_S3_ATTEMPTS:
            try:
                for item in self.iterate_bucket_items(bucket_name, object_prefix):
                    # logging.info(item)
                    if object_prefix == "":
                        suffix = item["Key"]
                    else:
                        suffix = item["Key"].split("/")[-2]
                    # logging.info(suffix)
                    search_result = re.search(search_expression, suffix)
                    # logging.info(item)
                    if search_result:
                        self.logger.info("match found")
                        logging.info(search_result.group(0))
                        object_exists = search_result.group(0)
                        self.logger.info(item)
                        self.logger.info(object_exists)
                        break

                else:
                    self.logger.info("no match found")
                    return TaskStatus(False, "object does not exist", None)

            except botocore.exceptions.ClientError as e:
                self.logger.info(
                    f"encountered an exception when accessing the s3 server: {e}"
                )
                if e.response["Error"]["Code"] == "404":
                    self.logger.info("object does not exist")
                    return TaskStatus(False, "object does not exist", e)
                else:
                    self.logger.info(
                        "Unable to determine if tile exists in local S3 repo"
                    )
                    self.logger.info(
                        f"Trying again ({current_attempt}/{self.MAX_S3_ATTEMPTS}"
                    )
                    current_attempt += 1
                    if current_attempt == self.MAX_S3_ATTEMPTS:
                        return TaskStatus(
                            False, "unable to communicate with S3 bucket", None
                        )

                    self.logger.info(f"Waiting {time_to_wait} before trying again...")
                    time.sleep(time_to_wait)
                    time_to_wait += 60
            else:
                return TaskStatus(True, object_exists, None)

    def upload_file_s3(self, full_file_path, prefix, bucket_name, callback=None):
        full_path = full_file_path         
        current_attempt = 0
        time_to_wait = self.WAIT_TIME

        print(full_path)
        print(full_path.name)
        print(bucket_name)
        while current_attempt < self.MAX_S3_ATTEMPTS:
            self.logger.info(
                f"attempting upload of {full_path} to in S3 bucket named {bucket_name}"
            )
            try:
                self.s3_client.upload_file(
                    str(full_path), bucket_name, full_path.name, Callback=callback
                )
            except BaseException as e:
                self.logger.warning(str(e))
                self.logger.info("upload failed because of S3 bucket problem")
                current_attempt += 1
                self.logger.info(
                    f"Trying again ({current_attempt}/{self.MAX_S3_ATTEMPTS})"
                )
                self.logger.info(f"Waiting {time_to_wait} before trying again...")
                time.sleep(time_to_wait)
            else:
                self.logger.info("Upload succeeded!")
                return TaskStatus(True, "Upload succeeded!", "")
        else:
            self.logger.info("Max attempts reached, s3 problem (unreachable?)")
            return TaskStatus(
                False, "Max attempts reached, s3 problem (unreachable?)", e
            )

    def check_archive_exists_in_s3(self, tile_name: str, bucket_name: str) -> bool:

        current_attempt = 0
        time_to_wait = self.WAIT_TIME

        full_tile_path = tile_name + ".zip"

        self.logger.info(full_tile_path)

        while current_attempt < self.MAX_S3_ATTEMPTS:
            try:
                self.s3_resource.Object(bucket_name, full_tile_path).load()
            except botocore.exceptions.ClientError as e:
                self.logger.info(
                    f"encountered an exception when accessing the s3 server: {e}"
                )
                if e.response["Error"]["Code"] == "404":
                    self.logger.info("object does not exist")
                    return False
                else:
                    self.logger.info(
                        "Unable to determine if tile exists in local S3 repo"
                    )
                    self.logger.info(
                        f"Trying again ({current_attempt}/{self.MAX_S3_ATTEMPTS}"
                    )
                    current_attempt += 1
                    if current_attempt == self.MAX_S3_ATTEMPTS:
                        return False

                    self.logger.info(f"Waiting {time_to_wait} before trying again...")
                    time.sleep(time_to_wait)
                    time_to_wait += 60

            else:
                return True

    def download_tile_from_s3_date_tile(self, current_date, tile_name, bucket_name):
        """Given a tile name, download the TIF from the correct location.

        TIF should be placed in the 'working_folder'
        """
        # L1C_T14UNV_A015210_20180521T173646
        # search_expression = r'L1C_{}_A\d{{6}}_{}T\d{{6}}.zip'.format(tile_name, current_date)
        search_expression = r"S2[AB]_MSIL1C_{}T\d{{6}}_N\d{{4}}_R\d{{3}}_T{}_\d{{8}}T\d{{6}}.zip".format(
            current_date, tile_name
        )

        object_exists = self.check_object_exists_in_s3_wildcards(
            search_expression, "", bucket_name
        )

        current_attempt = 0
        time_to_wait = self.WAIT_TIME

        self.logger.info("Trying to download L1C from S3...")
        self.logger.info(object_exists)

        if object_exists[0]:

            if Path(self.WORKING_FOLDER_PATH, object_exists[1]).exists():
                self.logger.info(
                    "Tile already exists in working_folder, not downloading"
                )
                return TaskStatus(
                    True,
                    "Tile already exists in working_folder, not downloading",
                    Path(object_exists[1]),
                )

            while current_attempt < self.MAX_S3_ATTEMPTS:

                self.logger.info("object found, downloading...")
                try:
                    self.logger.info(bucket_name)
                    self.logger.info(object_exists)

                    self.logger.info(str(Path(bucket_name, object_exists[1])))

                    self.s3_client.download_file(
                        bucket_name,
                        str(object_exists[1]),
                        str(Path(self.WORKING_FOLDER_PATH, object_exists[1])),
                    )

                except BaseException as e:
                    self.logger.info(e)
                    self.logger.info(
                        "Something went wrong with s3 bucket, unable to download from local repo"
                    )
                    self.logger.info(
                        f"Trying again ({current_attempt}/{self.MAX_S3_ATTEMPTS}"
                    )
                    current_attempt += 1

                    self.logger.info(f"Waiting {time_to_wait} before trying again...")
                    time.sleep(time_to_wait)
                    time_to_wait += 60
                else:
                    return TaskStatus(True, "", str(object_exists[1]))
            else:
                return TaskStatus(
                    False, "problems communicating with the local S3 repo", ""
                )

        else:
            return TaskStatus(False, "object does not exist", "")

    def download_tile_from_s3(self, tile_name, bucket_name, input_folder):
        """Given a tile name, download the TIF from the correct location.

        TIF should be placed in the 'working_folder'
        """

        obj_exists = None
        current_attempt = 0
        time_to_wait = self.WAIT_TIME

        full_tile_name = tile_name + ".zip"
        full_tile_path = Path(full_tile_name)

        self.logger.info(full_tile_name)
        self.logger.info(full_tile_path)

        self.logger.info(Path(self.WORKING_FOLDER_PATH, full_tile_path))
        self.logger.info(Path(self.WORKING_FOLDER_PATH, full_tile_path).exists())

        if Path(self.WORKING_FOLDER_PATH, full_tile_path).exists():
            self.logger.info("Tile already exists in working_folder, not downloading")
            return TaskStatus(
                True,
                "Tile already exists in working_folder, not downloading",
                full_tile_path,
            )

        obj_exists = self.check_archive_exists_in_s3(tile_name, bucket_name)

        if obj_exists:

            while current_attempt < self.MAX_S3_ATTEMPTS:

                self.logger.info("object found, downloading...")
                try:
                    self.s3_resource.Bucket(bucket_name).download_file(
                        str(full_tile_path), str(Path(input_folder, full_tile_name))
                    )
                except:
                    self.logger.info(
                        "Something went wrong with s3 bucket, unable to download from local repo"
                    )
                    self.logger.info(
                        f"Trying again ({current_attempt}/{self.MAX_S3_ATTEMPTS}"
                    )
                    current_attempt += 1
                    self.logger.info(f"Waiting {time_to_wait} before trying again...")
                    time.sleep(time_to_wait)
                    time_to_wait += 60
                else:
                    return TaskStatus(True, "", full_tile_path)
            else:
                return TaskStatus(
                    False, "problems communicating with the local S3 repo", ""
                )

        else:
            return TaskStatus(False, "object does not exist", "")

    def download_l2a_from_s3(self, prefix, bucket_name, destination_folder=None):
        """Given a tile name, download the TIF from the correct location.

        TIF should be placed in the 'working_folder'
        """

        obj_exists = None
        current_attempt = 0
        time_to_wait = self.WAIT_TIME

        if Path(self.WORKING_FOLDER_PATH, prefix).exists():
            self.logger.info("Tile already exists in working_folder, not downloading")
            return TaskStatus(
                True, "Tile already exists in working_folder, not downloading", prefix
            )

        self.logger.info(
            f"Trying to download the entirety of {prefix} from bucket {bucket_name}"
        )

        if next(self.iterate_bucket_items(bucket_name, prefix), None) is None:
            self.logger.info(
                "Probable problem with S3 key, no items to iterate, returning..."
            )
            return TaskStatus(
                False,
                "No Items to iterate over for prefix provided, path must be wrong.",
                None,
            )

        prefix_target = prefix.split("/")[-1]
        self.logger.info(prefix_target)
        # last item, find index, use to remove path for local file

        for obj in self.iterate_bucket_items(bucket_name, prefix):

            while current_attempt < self.MAX_S3_ATTEMPTS:

                self.logger.info("object found, downloading...")

                s3_object = obj["Key"]

                self.logger.info(s3_object)

                prefix_target_index = 0
                s3_object_elements = s3_object.split("/")
                for idx, ele in enumerate(s3_object_elements):
                    if ele == prefix_target:
                        self.logger.info(ele)
                        self.logger.info(prefix_target)
                        prefix_target_index = idx
                        break
                self.logger.info(prefix_target_index)
                no_prefix_obj = "/".join(s3_object.split("/")[prefix_target_index:])
                self.logger.info(no_prefix_obj)
                parent_path = Path(no_prefix_obj).parent
                self.logger.info(parent_path)

                if not Path(destination_folder, parent_path).exists():
                    os.makedirs(Path(destination_folder, parent_path))

                # Config=boto3.s3.transfer.TransferConfig(use_threads=False)
                if not Path(destination_folder, no_prefix_obj).exists():
                    try:
                        self.s3_client.download_file(
                            bucket_name,
                            obj["Key"],
                            str(Path(destination_folder, no_prefix_obj)),
                        )
                    except BaseException as e:
                        self.logger.info(e)
                        self.logger.info(
                            "Something went wrong with s3 bucket, unable to download from local repo"
                        )
                        self.logger.info(
                            f"Trying again ({current_attempt}/{self.MAX_S3_ATTEMPTS}"
                        )
                        current_attempt += 1
                        self.logger.info(
                            f"Waiting {time_to_wait} before trying again..."
                        )
                        time.sleep(time_to_wait)
                        time_to_wait += 60
                    else:
                        self.logger.info("download successful")
                        break
                else:
                    self.logger.info("file already downloaded by another process")
                    break
            else:
                return TaskStatus(
                    False, "problems communicating with the local S3 repo", ""
                )
        else:
            return TaskStatus(True, "Download successful", prefix_target)

    def upload_single_file_to_s3(self, file_name, bucket_name, callback=None, celery_task=None):

        full_path = Path(self.WORKING_FOLDER_PATH, file_name)
        current_attempt = 0
        time_to_wait = self.WAIT_TIME

        config = TransferConfig(use_threads=False)

        while current_attempt < self.MAX_S3_ATTEMPTS:
            self.logger.info(
                f"attempting upload of {full_path} to in S3 bucket named {bucket_name}"
            )
            try:
                self.s3_client.upload_file(
                    str(full_path), bucket_name, file_name, Callback=callback, Config=config
                )
            except:
                self.logger.info("upload failed because of S3 bucket problem")
                current_attempt += 1
                self.logger.info(
                    f"Trying again ({current_attempt}/{self.MAX_S3_ATTEMPTS})"
                )
                self.logger.info(f"Waiting {time_to_wait} before trying again...")
                time.sleep(time_to_wait)
                time_to_wait += 10
            else:
                self.logger.info("Upload succeeded!")
                return TaskStatus(True, "Upload succeeded!", "")
        else:
            self.logger.info("Max attempts reached, s3 problem (unreachable?)")
            return TaskStatus(
                False, "Max attempts reached, s3 problem (unreachable?)", ""
            )

    def upload_unarchived_product_to_s3_bucket(
        self, folder_path, bucket_name, callback=None
    ):
        """Upload the folder and contents to the S3 bucket

        """

        self.logger.info("trying to upload to s3")
        self.logger.info(folder_path.exists())

        product_name = folder_path.name
        self.logger.info(folder_path)
        self.logger.info(product_name)
        product_parts = product_name.split("_")

        config = TransferConfig(use_threads=False)
        
        if product_name.startswith('LC08'):
            tile = product_parts[2]
            self.logger.info(tile)
            path = tile[0:3]
            row = tile[3:]
            # make a path
            upload_path = Path("tiles", path, row)

        else:
            tile = product_parts[-2]
            self.logger.info(tile)
            utm_zone = tile[1:3]
            utm_band = tile[3]
            sqr_100km = tile[4:]

            # make a path
            upload_path = Path("tiles", utm_zone, utm_band, sqr_100km)

        for root, dirs, files in os.walk(folder_path):

            for f in files:
                partial_path = get_partial_path(Path(root, f), product_name)

                current_attempt = 0
                time_to_wait = self.WAIT_TIME

                while current_attempt < self.MAX_S3_ATTEMPTS:
                    self.logger.info(
                        f"attempting upload of {Path(root, f)} to {partial_path} in S3 bucket named {bucket_name}"
                    )
                    src_path = str(Path(root, f))
                    dst_path = str(Path(upload_path, partial_path))

                    dst_path_conv = dst_path.replace("\\", "/")

                    try:
                        # self.s3_resource.Bucket(bucket_name).upload_file(
                        #     src_path, dst_path_conv, Callback=callback
                        # )
                        self.s3_client.upload_file(
                            src_path, bucket_name, dst_path_conv, Callback=callback, Config=config
                        )

                    except BaseException as e:
                        self.logger.info(e)
                        self.logger.info("upload failed because of S3 bucket problem")
                        current_attempt += 1
                        self.logger.info(
                            f"Trying again ({current_attempt}/{self.MAX_S3_ATTEMPTS})"
                        )
                        self.logger.info(
                            f"Waiting {time_to_wait} before trying again..."
                        )
                        time.sleep(time_to_wait)
                        time_to_wait += 60
                    else:
                        self.logger.info("Upload succeeded!")
                        break
                else:
                    self.logger.info("Max attempts reached, s3 problem (unreachable?)")
                    return TaskStatus(False, "Max attempts reached, s3 problem", "")

        return TaskStatus(True, "All files uploaded", "")

    def upload_unarchived_product_to_s3(self, folder_path, prefix, bucket_name):

        self.logger.info("starting upload to s3")

        for root, dirs, files in os.walk(folder_path):

            for f in files:
                partial_path = get_partial_path(Path(root, f), folder_path.name)

                current_attempt = 0
                time_to_wait = self.WAIT_TIME

                while current_attempt < self.MAX_S3_ATTEMPTS:
                    self.logger.info(
                        f"attempting upload of {Path(root, f)} to {partial_path} in S3 bucket named {bucket_name}"
                    )
                    src_path = str(Path(root, f))
                    dst_path = str(Path(prefix, partial_path))

                    dst_path_conv = dst_path.replace("\\", "/")

                    try:
                        self.s3_client.upload_file(src_path, bucket_name, dst_path_conv)

                    except BaseException as e:
                        self.logger.info(e)
                        self.logger.info("upload failed because of S3 bucket problem")
                        current_attempt += 1
                        self.logger.info(
                            f"Trying again ({current_attempt}/{self.MAX_S3_ATTEMPTS})"
                        )

                        self.logger.info(
                            f"Waiting {time_to_wait} before trying again..."
                        )
                        time.sleep(time_to_wait)
                        time_to_wait += 60
                    else:
                        self.logger.info("Upload succeeded!")
                        break
                else:
                    self.logger.info("Max attempts reached, s3 problem (unreachable?)")
                    return TaskStatus(False, "Max attempts reached, s3 problem", "")

        return TaskStatus(True, "All files uploaded", "")


class S3ConfigFileProblem(Exception):
    pass


class S3ResourceConnectionProblem(Exception):
    pass


class S3ClientConnectionProblem(Exception):
    pass
