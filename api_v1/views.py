# api_v1/views

import datetime
import humps
import json
import logging
import pytz
from pathlib import Path
from tabulate import tabulate

import requests

import asyncio

from django.utils import timezone

from rest_framework import status
from rest_framework import filters

from django.contrib.auth.models import User

from celery.result import AsyncResult

from django.conf import settings

from django.shortcuts import get_object_or_404, get_list_or_404
from django.db.models import Q
from django.db import transaction

from rest_framework import generics, permissions, renderers
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework.reverse import reverse

from django_filters.rest_framework import DjangoFilterBackend
from rest_framework.views import APIView

from django.contrib.auth.models import User
from .models import Job
from .permissions import IsOwnerOrReadOnly
from .serializers import (
    JobSerializer,
    JobCreateSerializer,
    UserSerializer,
    JobStatusSerializer,
)

from .models import Worker
from .serializers import WorkerSerializer, WorkerCreateSerializer
from .serializers import WorkerJobCompleteSerializer
from .models import JOB_STATUSES, JOB_PRIORITIES
from .models import Job

from api_v1.models import JobStatusChoice

from worker.tasks.download_s2 import download_s2
from worker.tasks.download_l8 import download_l8

from worker.tasks.sen2agri_tasks import start_l2a_job

from common.s3_utils import S3Utility, S3UtilityAio

from .tasks import start_s2_batch_job, start_l8_batch_job

# create logger
module_logger = logging.getLogger("api_v1.views")

s3_helper = S3Utility(settings.S3_CONFIG, Path(settings.BASE_DIR, "working_folder"))
s3_helper_aio = S3UtilityAio(
    settings.S3_CONFIG, Path(settings.BASE_DIR, "working_folder")
)

import redis

redis_instance = redis.StrictRedis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=0,
    password=settings.REDIS_PASS,
)

SENTINEL_L1C_BUCKETNAME = "s2-l1c-archive"
SEN2AGRI_L2A_BUCKETNAME = "sen2agri-l2a"


# class GetJob(generics.GenericAPIView):
#     queryset = Job.objects.filter(status="S").order_by("-submitted", "-priority")
#     renderer_classes = (renderers.JSONRenderer,)

#     def get(self, request, *args, **kwargs):
#         # Check for valid worker_id and set worker capabilities
#         worker_id = self.request.query_params.get("worker_id", None)
#         worker_capabilities = None
#         print(f"worker_id: {worker_id}")
#         try:
#             worker = Worker.objects.get(pk=worker_id)
#         except Worker.DoesNotExist:
#             return Response(
#                 {"info": f"Invalid 'worker_id'"}, status=status.HTTP_403_FORBIDDEN
#             )
#         else:
#             worker_capabilities = worker.capabilities
#             if len(self.get_queryset()) > 0:
#                 print(worker_capabilities)
#                 # Filter by worker capabilities before getting a job
#                 q_obj = Q(job_type__startswith="L8Download") & Q(
#                     parameters__options__ac=True
#                 )
#                 query_test = self.get_queryset().filter(q_obj)
#                 for obj in query_test:
#                     print(obj)

#                 query_results = (
#                     self.get_queryset()
#                     .exclude(q_obj)
#                     .filter(job_type__in=worker_capabilities)
#                 )

#                 for obj in query_results:
#                     print(obj)

#                 if len(query_results) > 0:
#                     job = query_results[0]

#                     serialized_job = JobCreateSerializer(
#                         job, context={"request": request}
#                     )
#                     print(serialized_job.data)

#                     current_job = Job.objects.get(pk=job.pk)
#                     current_job.status = "A"
#                     current_job.assigned = timezone.now()
#                     current_job.worker_id = worker
#                     current_job.save()

#                     print(current_job.status)

#                     return Response(serialized_job.data, status=status.HTTP_200_OK)
#                 else:
#                     return Response(
#                         {
#                             "info": "No Jobs currently available. If you expected jobs, verify that this worker has the necessary capabilities."
#                         },
#                         status=status.HTTP_200_OK,
#                     )
#             else:
#                 return Response(
#                     {"info": "No Jobs currently available"}, status=status.HTTP_200_OK
#                 )


class RegisterWorker(generics.GenericAPIView):
    # queryset = Job.objects.filter(status='S').order_by('-submitted', '-priority')
    renderer_classes = (renderers.JSONRenderer,)
    serializer_class = WorkerCreateSerializer

    def perform_create(self, serializer):
        # serializer.save(owner=self.request.user)
        pass

    def post(self, request, *args, **kwargs):
        print(request)
        print("Trying to POST register a new worker")

        # if len(self.get_queryset()) > 0:
        #     job = self.get_queryset()[0]
        #     serialized_job = JobSerializer(job, context={'request': request})
        #     print(serialized_job.data)
        server_code_valid = False

        serialized_worker = WorkerCreateSerializer(data=request.data)

        if settings.SERVER_CODE and "server_code" in request.data:
            server_code_valid = settings.SERVER_CODE == request.data["server_code"]
        else:
            server_code_invalid = "Missing or invalid server code"

        if serialized_worker.is_valid() and server_code_valid:

            try:
                existing_worker = Worker.objects.get(
                    worker_name__exact=request.data["worker_name"]
                )
            except Worker.DoesNotExist:
                serialized_worker.save()
                return Response({"worker_id": serialized_worker.data["id"]}, status=201)
            else:
                return Response(
                    {
                        "worker_id": existing_worker.id,
                        "info": f"Worker with name {serialized_worker.data['worker_name']} already exists.",
                    },
                    status=200,
                )

        else:
            return Response(
                {"errors": serialized_worker.errors or server_code_invalid}, status=400
            )


@api_view(["GET"])
def api_root(request, format=None):
    return Response(
        {
            "users": reverse("user-list", request=request, format=format),
            "jobs": reverse("job-list", request=request, format=format),
            "workers": reverse("worker-list", request=request, format=format),
        }
    )


class WorkerList(generics.ListAPIView):
    queryset = Worker.objects.all()
    serializer_class = WorkerCreateSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly,)

    def perform_create(self, serializer):
        serializer.save(owner=self.request.user)

    def get(self, request, format=None):
        workers = Worker.objects.all()
        serializer = WorkerSerializer(workers, context={"request": request}, many=True)

        return Response(serializer.data)


class WorkerDetail(generics.RetrieveUpdateDestroyAPIView):
    queryset = Worker.objects.all()
    serializer_class = WorkerSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsOwnerOrReadOnly)

def handle_l8_batch_job_submission(parameters, request_user, serializer=None, job=None):
    # Convert from camelCase to snake_case
    parameters = humps.decamelize(parameters)
    module_logger.info(parameters)
    tile_list = parameters["tile_list"]

    module_logger.info(tile_list)
    module_logger.info(request_user)

    task_id = start_l8_batch_job.delay(
        tile_list,
        parameters["ac"],
        parameters["ac_res"],
        str(request_user),
    )

    module_logger.info(task_id)
    if serializer:
        job = serializer.save(
            owner=request_user, task_id=str(task_id), parameters=parameters
        )

    # Send email notification for job recieved
    #  "aoi_name": "Test Site 1",
    # "submitted": "2020-04-18T01:43:49.618761Z",
    # "assigned": "2020-04-18T01:47:08.363458Z",
    # "completed": "2020-04-18T01:47:18.354564Z",
    # "label": "S2BatchDownload",
    # "command": "not used",
    # "job_type": "S2BatchDownload",
    # "task_id": "406882a4-5850-4413-8fc6-f90ebcfbbab6",
    # "parameters": {
    #     "ac": true,
    #     "ac_res": [
    #         10
    #     ],
    #     "tile_list": [
    #         {
    #             "api": "esa_scihub",
    #             "mgrs": "11UQR",
    #             "name": "S2A_MSIL1C_20190723T182921_N0208_R027_T11UQR_20190723T220633",
    #             "tile_id": "0ffa9a18-76e1-4325-aad8-a65443d9deab",
    #             "entity_id": "46fee41d-d390-423b-b3c9-beac24eacdc8",
    #             "acquisition_date": "2019-07-24T00:29:21.024Z"
    #         }
    #     ]
    # }
    tabulate_header = [
        "Tile Name",
        "API",
        "API Tile ID",
    ]

    module_logger.info(tabulate_header)

    tabulate_list = [[t["name"], t["api"], t["entity_id"],] for t in tile_list]
    module_logger.info(tabulate_list)
    user = User.objects.get(username=request_user)
    user_email = user.email

    mail_subject = f"L8 Batch Download Job Received ({job.id})"

    mail_text = "<html><body><p>Great news! Your L8 Batch Download job has been recieved and is under way. You will receive another email when the job is complete!</p>\n\n"

    mail_text += f"<pre>Job ID: {job.id}\nAOI Name: {job.aoi_name}\nSubmitted: {str(job.submitted)}\n"
    mail_text += f"Params:\n"
    mail_text += f"ac: {parameters['ac']}\nac_res: {parameters['ac_res']}\n\n"
    mail_text += (
        tabulate(tabulate_list, headers=tabulate_header)
        + "</pre></body></html>"
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

    module_logger.info("Recieved L8 Batch Job")
    module_logger.info("Sending email using mailgun")

    return task_id

def handle_s2_batch_job_submission(parameters, request_user, serializer=None, job=None):
     # Dispatch handleS2BatchJobCreation task
    # get task_id from celery task
    # add task_id to job
    module_logger.info("Recieved a S2BatchDownload request")

    # Convert from camelCase to snake_case
    parameters = humps.decamelize(parameters)
    module_logger.info(parameters)
    tile_list = parameters["tile_list"]

    module_logger.info(tile_list)
    module_logger.info(request_user)

    task_id = start_s2_batch_job.delay(
        tile_list,
        parameters["ac"],
        parameters["ac_res"],
        str(request_user),
    )

    module_logger.info(task_id)

    if serializer:
        job = serializer.save(
            owner=request_user, task_id=str(task_id), parameters=parameters
        )

    # Send email notification for job recieved
    #  "aoi_name": "Test Site 1",
    # "submitted": "2020-04-18T01:43:49.618761Z",
    # "assigned": "2020-04-18T01:47:08.363458Z",
    # "completed": "2020-04-18T01:47:18.354564Z",
    # "label": "S2BatchDownload",
    # "command": "not used",
    # "job_type": "S2BatchDownload",
    # "task_id": "406882a4-5850-4413-8fc6-f90ebcfbbab6",
    # "parameters": {
    #     "ac": true,
    #     "ac_res": [
    #         10
    #     ],
    #     "tile_list": [
    #         {
    #             "api": "esa_scihub",
    #             "mgrs": "11UQR",
    #             "name": "S2A_MSIL1C_20190723T182921_N0208_R027_T11UQR_20190723T220633",
    #             "tile_id": "0ffa9a18-76e1-4325-aad8-a65443d9deab",
    #             "entity_id": "46fee41d-d390-423b-b3c9-beac24eacdc8",
    #             "acquisition_date": "2019-07-24T00:29:21.024Z"
    #         }
    #     ]
    # }
    tabulate_header = [
        "Tile Name",
        "API",
        "API Tile ID",
    ]

    module_logger.info(tabulate_header)

    tabulate_list = [[t["name"], t["api"], t["entity_id"],] for t in tile_list]
    module_logger.info(tabulate_list)
    user = User.objects.get(username=request_user)
    user_email = user.email

    mail_subject = f"S2 Batch Download Job Received ({job.id})"

    mail_text = "<html><body><p>Great news! Your S2 Batch Download job has been recieved and is under way. You will receive another email when the job is complete!</p>\n\n"

    mail_text += f"<pre>Job ID: {job.id}\nAOI Name: {job.aoi_name}\nSubmitted: {str(job.submitted)}\n"
    mail_text += f"Params:\n"
    mail_text += f"ac: {parameters['ac']}\nac_res: {parameters['ac_res']}\n\n"
    mail_text += (
        tabulate(tabulate_list, headers=tabulate_header)
        + "</pre></body></html>"
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

    module_logger.info("Recieved S2 Batch Job")
    module_logger.info("Sending email using mailgun")

    return task_id


class JobList(generics.ListCreateAPIView):
    serializer_class = JobCreateSerializer
    permission_classes = (IsAuthenticated,)

    filter_backends = (filters.OrderingFilter, DjangoFilterBackend)
    filterset_fields = ("status", "success", "owner", "job_type")
    ordering_fields = ("priority", "submitted", "assigned", "completed")

    def query_params_parser(self, fields_list):
        """Convenience func for converting query_parafrom django.db.models import Q
ms to python Dict"""
        json_values_dict = {}

        for field in fields_list:
            value = (
                self.request.query_params.get(field)
                if not self.request.query_params.get(field, "") == ""
                else "null"
            )
            json_values_dict['"' + field + '"'] = value

        json_string = "{"

        for idx, (key, value) in enumerate(json_values_dict.items()):
            if value not in ["null", "true", "false"]:
                json_string += f'{key}: "{value}"'
            else:
                json_string += f"{key}: {value}"

            if idx < len(json_values_dict) - 1:
                json_string += ","

        json_string += "}"

        module_logger.debug(json_string)

        final_dict = json.loads(json_string)

        return final_dict

    def perform_create(self, serializer):
        module_logger.info("Inside perform create.")

        validated_data = serializer.validated_data
        module_logger.info(f"Validated data: {validated_data}")
        parameters = validated_data.get("parameters")
        job_type = validated_data.get("job_type")
        aoi_name = validated_data.get("aoi_name")

        module_logger.info(parameters)
        module_logger.info(job_type)

        task_id = None
        task_id_list = None
        task_info = None
        ac_enabled = False

        ac_enabled = parameters["ac"]

        if job_type == "S2Download":
            module_logger.info("Adding S2 Download Task to ondemand queue")
            task_id = download_s2.delay(parameters)
            task_info = AsyncResult(task_id).info
            module_logger.info(task_info)

        elif job_type == "L8Download" and not ac_enabled:
            module_logger.info("Adding L8 Download Task (no-ac) to ondemand queue")
            task_id = download_l8.delay(parameters)

        elif job_type == "Sen2Agri_L2A":
            module_logger.info("Adding Sen2Agri_L2A Task to on demand queue")
            module_logger.info(parameters)
            imagery_list = parameters["options"]["imagery_list"]
            platform_list = imagery_list.keys()

            imagery_list_no_empty_dates = {}

            # remove dates from the list that have no imagery selected
            for platform in platform_list:
                date_list = imagery_list[platform]
                dates = date_list.keys()
                imagery_list_no_empty_dates[platform] = {}

                for d in dates:
                    if len(date_list[d]):
                        imagery_list_no_empty_dates[d] = date_list[d]

            platform_list = ["landsat8", "sentinel2"]
            imagery_list_no_empty_dates = {}

            task_ids = start_l2a_job(
                parameters["options"]["imagery_list"],
                "na",
                parameters["options"]["aoi_name"],
                parameters["options"]["window_size"],
            )

            module_logger.info(task_ids)

            task_id_list = task_ids
            module_logger.info(task_id)
        elif job_type == "S2BatchDownload":
            # Dispatch handleS2BatchJobCreation task
            # get task_id from celery task
            # add task_id to job
            module_logger.info("Recieved a S2BatchDownload request")
            task_id = handle_s2_batch_job_submission(parameters, self.request.user, serializer=serializer)

        elif job_type == "L8BatchDownload":
            # Dispatch handleS2BatchJobCreation task
            # get task_id from celery task
            # add task_id to job
            module_logger.info("Recieved a L8BatchDownload request")
            task_id = handle_l8_batch_job_submission(parameters, self.request.user, serializer=serializer)

    def get(self, request, format=None):
        # IMPORTANT: use self.get_queryset() any place you want filtering to be enabled
        # for built in filtering or ordering you must call self.filter_queryset
        job_id_string = request.GET.get("job_ids")

        if job_id_string:
            module_logger.debug("job ids were included with the request")
            module_logger.debug(job_id_string)
            job_id_list = job_id_string.split(",")
            jobs = get_list_or_404(self.get_queryset(), id__in=job_id_list)

            serializer = JobStatusSerializer(
                jobs, many=True, context={"request": request}
            )

            return Response(serializer.data, status=status.HTTP_200_OK)
        else:
            jobs = self.filter_queryset(self.get_queryset())
            page = self.paginate_queryset(jobs)

            if page is not None:
                serializer = JobSerializer(
                    page, context={"request": request}, many=True
                )
                return self.get_paginated_response(serializer.data)

            serializer = JobSerializer(jobs, context={"request": request}, many=True)

            return Response(serializer.data)

    def get_queryset(self):
        """
        This view should return a list of all the purchases
        for the currently authenticated user.
        """
        queryset = Job.objects.all()

        # parse json query_params
        # query_params, list of fields
        # returns dict

        params_dict = self.query_params_parser(
            ["status", "success", "owner", "job_type"]
        )

        status = params_dict["status"]
        success = params_dict["success"]
        owner = params_dict["owner"]
        job_type = params_dict["job_type"]

        if status is not None:
            queryset = queryset.filter(status=status)
        if success is not None:
            queryset = queryset.filter(success=success)
        if owner is not None:
            queryset = queryset.filter(owner=owner)
        if job_type is not None:
            queryset = queryset.filter(job_type=job_type)

        return queryset


class ImageryStatus(generics.GenericAPIView):
    """Utility end point to fetch the status for imagery for a specific AoI

    NOTE: a major improvement would be a AoI model that would store tile info
    for each AoI. As it stands, the required params are:
        aoi name
        data type (l1c, l2a, sen2agri_l2a)
        imagery_name_list
    """

    async def get_imagery_status_for_l1c(self, imagery_list):

        tasks = []

        for image_name in imagery_list:
            name_parts = image_name.split("_")
            usgs_search_expression = r"L1C_{}_A\d{{6}}_{}".format(
                name_parts[5], name_parts[2]
            )

            usgs_exists = asyncio.ensure_future(
                s3_helper_aio.check_object_exists_in_s3_wildcards(
                    usgs_search_expression,
                    "",
                    SENTINEL_L1C_BUCKETNAME,
                    image_name=image_name,
                    api_source="usgs",
                )
            )

            tasks.append(usgs_exists)

            
            # S2B_MSIL1C_20190708T182929_N0208_R027_T11UQR_20190708T215643.zip
            print(name_parts)
            esa_search_expression = r"S2[A|B]_MSIL1C_{}_{}_{}_{}_\d{{8}}T\d{{6}}".format(
                name_parts[2], name_parts[3], name_parts[4], name_parts[5]
            )
            esa_exists = asyncio.ensure_future(
                s3_helper_aio.check_object_exists_in_s3_wildcards(
                    esa_search_expression,
                    "",
                    SENTINEL_L1C_BUCKETNAME,
                    image_name=image_name,
                    api_source="esa",
                )
            )
            tasks.append(esa_exists)

        imagery_status_results = await asyncio.gather(*tasks, return_exceptions=True)
        module_logger.warning(imagery_status_results)

        return imagery_status_results

    async def get_imagery_status_for_sen2agri_l2a(self, imagery_list, aoi_name):

        tasks = []

        for image_name in imagery_list:
            name_parts = image_name.split("_")
            search_expression = r"SENTINEL2{}_{}-\d{{6}}-\d{{3}}_L2A_{}_C_V1-0".format(
                name_parts[0][2], name_parts[2][:8], name_parts[5]
            )

            l2a_exists = asyncio.ensure_future(
                s3_helper_aio.check_object_exists_in_s3_wildcards(
                    search_expression,
                    str(Path(aoi_name, "sentinel2")),
                    SEN2AGRI_L2A_BUCKETNAME,
                    image_name,
                )
            )

            tasks.append(l2a_exists)

        imagery_status_results = await asyncio.gather(*tasks, return_exceptions=True)
        module_logger.warning(imagery_status_results)
        return imagery_status_results

    def get(self, request, *args, **kwargs):
        aoi_name = self.request.query_params.get("aoi_name", None)
        data_type = self.request.query_params.get("data_type", None)
        imagery_list = self.request.query_params.get("imagery_list", None).split(",")
        module_logger.info(
            f"received request to check imagery, {aoi_name} - {data_type} - {imagery_list}"
        )
        # Imagery in L1C archive can be in the USGS form
        # L1C_T20UMV_A021615_20190812T151806.zip
        # S2A_MSIL1C_20190824T154911_N0208_R054_T18UYV_20190824T192950.zip
        response = Response(
            {"error": f"invalid type for imagery provided ({data_type})"}
        )

        # Check object exists L1C using check_object_exists_in_s3_wildcards
        if data_type == "l1c":
            imagery_status_dict = {}
            # imagery_status_dict["l1c"] = self.get_imagery_status_for_l1c(imagery_list)
            l1c_result = asyncio.run(self.get_imagery_status_for_l1c(imagery_list))

            imagery_status_l1c = {}

            for image_dict in l1c_result:
                key = list(image_dict)[0]
                internal_dict = image_dict[key]
                internal_key = list(internal_dict)[0]
                if key in imagery_status_l1c:
                    imagery_status_l1c[key][internal_key] = internal_dict[internal_key]
                else:
                    imagery_status_l1c[key] = {}
                    imagery_status_l1c[key][internal_key] = internal_dict[internal_key]
            imagery_status_dict["l1c"] = imagery_status_l1c

            response = Response({"data": imagery_status_dict})

        elif data_type == "sen2agri_l2a":
            # Imagery format
            # SENTINEL2A_20190723-184025-003_L2A_T11UPS_C_V1-0/
            # S3 path sen2agri-l2a/sitename/sentinel2
            imagery_status_dict = {}

            l1c_result = asyncio.run(self.get_imagery_status_for_l1c(imagery_list))

            imagery_status_l1c = {}

            for image_dict in l1c_result:
                key = list(image_dict)[0]
                internal_dict = image_dict[key]
                internal_key = list(internal_dict)[0]
                if key in imagery_status_l1c:
                    imagery_status_l1c[key][internal_key] = internal_dict[internal_key]
                else:
                    imagery_status_l1c[key] = {}
                    imagery_status_l1c[key][internal_key] = internal_dict[internal_key]

            imagery_status_dict["l1c"] = imagery_status_l1c

            l2a_result = asyncio.run(
                self.get_imagery_status_for_sen2agri_l2a(imagery_list, aoi_name)
            )
            imagery_status_sen2agri_l2a = {}

            for image_dict in l2a_result:
                key = list(image_dict)[0]
                imagery_status_sen2agri_l2a[key] = image_dict[key]

            imagery_status_dict["sen2agri_l2a"] = imagery_status_sen2agri_l2a

            response = Response({"data": imagery_status_dict})

        return response


class RedisGetSetTest(generics.GenericAPIView):
    """Utility end point to fetch the status for imagery for a specific AoI

    NOTE: a major improvement would be a AoI model that would store tile info
    for each AoI. As it stands, the required params are:
        aoi name
        data type (l1c, l2a, sen2agri_l2a)
        imagery_name_list
    """

    def get(self, request, *args, **kwargs):

        key = self.request.query_params.get("key", None)
        value = self.request.query_params.get("value", None)
        get = self.request.query_params.get("get", None)

        if key:
            dict_item = {"thing": "thing", "number": 0, "value": value}

            redis_instance.hmset(key, dict_item)
            response = {"msg": f"{key} successfully set to {value}"}
            return Response(response, status=200)
        else:

            items = {}
            count = 0
            get = get if get else "*"

            for key in redis_instance.keys(get):
                key = key.decode("utf-8")
                type_of_value = redis_instance.type(key).decode("utf-8")
                print(key)
                print(type_of_value)
                if type_of_value == "string":
                    items[key] = redis_instance.get(key)
                else:
                    items[key] = redis_instance.hmget(key, ["thing", "number", "value"])
                count += 1

            response = {"count": count, "msg": f"Found {count} items.", "items": items}

            return Response(response, status=200)


class JobRetry(generics.GenericAPIView):
    """ Given a job id, if it exists, grab the parameters and attempt to resubmit the start task for that
    job type.
    """

    def get(self, request, *args, **kwargs):
        module_logger.info(self.kwargs["pk"])
        jobs = (
            Job.objects.select_for_update()
            .filter(
                Q(id__exact=self.kwargs["pk"]))
        )

        module_logger.info('Recieved Job Retry request...')
        
        try:
            with transaction.atomic():
                job = jobs[0]
                module_logger.info(job.task_id)
                job.info = {}
                job.assigned = None
                job.completed = None
                job.success = False
                job.result_message = None
                job.status = JobStatusChoice.SUBMITTED.value
                if job.job_type == "S2BatchDownload":
                    module_logger.info("Recieved a S2BatchDownload request")
                    task_id = handle_s2_batch_job_submission(job.parameters, job.owner, job=job)
                    job.task_id = str(task_id)
                    module_logger.info(f'New task id for job creation task: {task_id}')
                    job.save()
                elif job.job_type == "L8BatchDownload":
                    module_logger.info("Recieved a L8BatchDownload request")
                    task_id = handle_l8_batch_job_submission(job.parameters, job.owner, job=job)
                    job.task_id = str(task_id)
                    module_logger.info(f'New task id for job creation task: {task_id}')
                    job.save()
                else:
                    module_logger.warning(f"Job type of {job.job_type} does not support the RETRY endpoint.")
            
            return Response('OK', status=200)
        except BaseException as e:
            return Response(f'Exception occured while trying to retry job {e}', status=503)

class JobDetail(generics.RetrieveUpdateDestroyAPIView):
    queryset = Job.objects.all()
    serializer_class = JobSerializer
    permission_classes = (IsAuthenticated,)

    # permission_classes = (permissions.IsAuthenticatedOrReadOnly,
    #                       IsOwnerOrReadOnly,)

    def post(self, request, format=None, *args, **kwargs):
        module_logger.debug("INSIDE JOBDETAIL POST METHOD OVERRIDE")
        job = get_object_or_404(self.get_queryset(), pk=kwargs["pk"])

        if "worker_id" in request.data:
            module_logger.debug("worker_id present, no need for auth")
            module_logger.debug(request.data)
            completed_time = timezone.now()
            job.completed = completed_time
            serializer = WorkerJobCompleteSerializer(
                job, data=request.data, partial=True
            )

            if serializer.is_valid():
                module_logger.debug("serializer valid, saving")
                serializer.save()

                # job_type = serializer.data['job_type']

                # if job_type == 'Sen2Agri_L2A':

                return Response(serializer.data, status=status.HTTP_200_OK)
            else:
                module_logger.debug("something was wrong with the serializer")
                return Response(
                    {"info": serializer.errors}, status=status.HTTP_400_BAD_REQUEST
                )

        else:

            module_logger.debug("no worker_id present, auth required")

            try:
                user = User.objects.get(username=request.user)

            except:
                module_logger.debug("User does not exist")
                return Response(
                    "Username does not exist", status=status.HTTP_403_FORBIDDEN
                )

            else:
                module_logger.debug(f"authed username: {user.username}")
                module_logger.debug(f"job owner: {job.owner}")
                if str(user.username) == str(job.owner):
                    module_logger.debug("User authed and owns this job")
                    completed_time = timezone.now()
                    job.completed = completed_time
                    serializer = JobSerializer(
                        job,
                        data=request.data,
                        context={"request": request},
                        partial=True,
                    )

                    if serializer.is_valid():
                        module_logger.debug("serializer valid, saving")
                        serializer.save()

                        return Response(serializer.data, status=status.HTTP_200_OK)
                    else:
                        module_logger.debug("something was wrong with the serializer")
                        return Response(
                            {
                                "info": f"Something went wrong with the serializer: {serializer.errors}"
                            },
                            status=status.HTTP_400_BAD_REQUEST,
                        )
                else:
                    module_logger.debug("User is authed but does not own this job")
                    return Response(
                        "User is authed but does not own this job",
                        status=status.HTTP_403_FORBIDDEN,
                    )


class UserList(generics.ListAPIView):
    queryset = User.objects.all()
    serializer_class = UserSerializer
    permission_classes = (IsAuthenticated,)



class UserDetail(generics.RetrieveAPIView):
    queryset = User.objects.all()
    serializer_class = UserSerializer
    permission_classes = (IsAuthenticated,)



from django.middleware.csrf import get_token

# from .tasks import add
from jobmanager.celery import debug_task
from .tasks import add_numbers

# from jobmanager.celery import add_numbers2


class CSRFGeneratorView(APIView):
    def get(self, request):
        csrf_token = get_token(request)
        return Response(csrf_token)


class Version(APIView):
    def get(self, request):
        data = {"version": settings.API_VERSION}
        return Response(data)


class TestEndPoint(APIView):
    def get(self, request):
        number1 = self.request.query_params.get("number1", None)
        number2 = self.request.query_params.get("number2", None)
        print("help")
        # result = add.delay(number1, number2).get()
        result = "test"

        print(result)
        print(add_numbers)

        async_result = add_numbers.delay(int(number1), int(number2))
        result1 = async_result.get()

        data = {"result": result1}
        return Response(data)


import datetime

from rest_framework_simplejwt.views import TokenObtainPairView
from rest_framework_simplejwt.serializers import TokenObtainPairSerializer

SUPERUSER_LIFETIME = datetime.timedelta(minutes=1)
from rest_framework import serializers

from django.contrib.auth import authenticate


class LoginSerializer(serializers.Serializer):
    email = serializers.CharField(max_length=255)
    password = serializers.CharField(max_length=128, write_only=True)
    token = serializers.CharField(max_length=255, read_only=True)

    def validate(self, data):
        # The `validate` method is where we make sure that the current
        # instance of `LoginSerializer` has "valid". In the case of logging a
        # user in, this means validating that they've provided an email
        # and password and that this combination matches one of the users in
        # our database.
        email = data.get("email", None)
        password = data.get("password", None)

        # Raise an exception if an
        # email is not provided.
        if email is None:
            raise serializers.ValidationError("An email address is required to log in.")

        # Raise an exception if a
        # password is not provided.
        if password is None:
            raise serializers.ValidationError("A password is required to log in.")

        # The `authenticate` method is provided by Django and handles checking
        # for a user that matches this email/password combination. Notice how
        # we pass `email` as the `username` value since in our User
        # model we set `USERNAME_FIELD` as `email`.
        user = authenticate(username=email, password=password)

        # If no user was found matching this email/password combination then
        # `authenticate` will return `None`. Raise an exception in this case.
        if user is None:
            raise serializers.ValidationError(
                "A user with this email and password was not found."
            )

        # Django provides a flag on our `User` model called `is_active`. The
        # purpose of this flag is to tell us whether the user has been banned
        # or deactivated. This will almost never be the case, but
        # it is worth checking. Raise an exception in this case.
        if not user.is_active:
            raise serializers.ValidationError("This user has been deactivated.")

        # The `validate` method should return a dictionary of validated data.
        # This is the data that is passed to the `create` and `update` methods
        # that we will see later on.
        return {"email": user.email, "username": user.username, "token": user.token}


# class MyTokenObtainView(TokenObtainPairView):
#     login_serializer = LoginSerializer

#     class Meta:
#         model = User
#         fields = ("email", "password")
from rest_framework_simplejwt import views as jwt_views
from rest_framework_simplejwt import serializers as jwt_serializers


class CustomTokenObtainSerializer(jwt_serializers.TokenObtainSerializer):
    username_field = User.EMAIL_FIELD


class CustomTokenObtainPairSerializer(
    CustomTokenObtainSerializer, jwt_serializers.TokenObtainPairSerializer
):
    pass


class CustomTokenObtainPairView(jwt_views.TokenViewBase):
    """
    Takes a set of user credentials and returns an access and refresh JSON web
    token pair to prove the authentication of those credentials.
    """

    serializer_class = CustomTokenObtainPairSerializer


from django.contrib.auth import get_user_model
from django.contrib.auth.models import Permission
from django.db.models import Exists, OuterRef, Q

UserModel = get_user_model()
from django.contrib.auth.backends import ModelBackend


class CustomEmailAuthBackend(ModelBackend):
    def authenticate(self, request, username=None, password=None, **kwargs):
        module_logger.debug(username)

        if username is None:
            username = kwargs.get(UserModel.EMAIL_FIELD)
        if username is None or password is None:
            return
        try:
            user = UserModel._default_manager.get(email=username)
        except UserModel.DoesNotExist:
            # Run the default password hasher once to reduce the timing
            # difference between an existing and a nonexistent user (#20760).
            UserModel().set_password(password)
        else:
            if user.check_password(password) and self.user_can_authenticate(user):
                return user
