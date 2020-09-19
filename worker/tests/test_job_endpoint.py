from django.urls import reverse

from django.contrib.auth import models

from django.utils import timezone

from rest_framework import status
from rest_framework.test import APITestCase, APITransactionTestCase

from api_v1.models import Worker, Job

import datetime
import pytz

import json

from worker.tasks import sen2agri_tasks


class CeleryTasksTests(APITransactionTestCase):
    def setUp(self):
        self.imagery_list_example = {
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
                "20190721": [],
            }
        }

    # @override_settings(CELERY_TASK_ALWAYS_EAGER=True)
    def test_sen2agri_tasks(self):
        task = sen2agri_tasks.start_l2a_job.delay(
            self.imagery_list_example, "0", "Test AOI Name"
        )
        print(task)
        result = task.get()
        print(result)
        self.assertTrue(True)
