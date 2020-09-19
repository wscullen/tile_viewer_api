from django.urls import reverse

from django.contrib.auth import models

from rest_framework import status
from rest_framework.test import APITestCase

from api_v1.models import Worker, Job

import datetime
import pytz

import json

class RequestJobTests(APITestCase):

    def setUp(self):
        self.getjob_url = reverse('get-job')
        self.registerworker_url = reverse('register-worker')
        self.testserver_url_prefix = 'http://testserver'
        self.worker_data_allgood = {
            'worker_name': 'test_worker',
            'system_info': {
                'hostname': 'OTT-UBU-01',
                'system_platform': 'linux',
                'platform': 'Linux-4.15.0-45-generic-x86_64-with-Ubuntu-18.04-bionic',
                'cpu': 'x86_64',
                'os': 'Linux',
                'os_ver': '#48-Ubuntu SMP Tue Jan 29 16:28:13 UTC 2019',
                'os_rel': '4.15.0-45-generic',
                'cores': 4
            },
            'capabilities': [
                'S1_Preprocessing_v1', 'S2_Atmoscor_v1'
            ],
            'server_code': 'aafc'
        }

        # pre-a: Create a user to associate with the job
        self.user = models.User.objects.create_user(username='testuser', password='12345')

        print(self.user)

        # 1. Register a worker
            # Create a worker using Django model ORM

        self.worker = Worker.objects.create(worker_name=self.worker_data_allgood['worker_name'],
                                            system_info=json.dumps(self.worker_data_allgood['system_info']),
                                            capabilities=self.worker_data_allgood['capabilities'],
                                            schedule="{}",
                                            )

        self.worker_missing_capabilities = Worker.objects.create(worker_name='test_worker_bad_capabilities',
                                            system_info=json.dumps(self.worker_data_allgood['system_info']),
                                            capabilities=['UnknownCapability_v1'],
                                            schedule="{}",
                                            )

        # 2. Create a job
        self.job = Job.objects.create(label="S1 Preprocessing 1_S1_FullSceneTest_Orb_Sig0_GM7x7_RD Carman",
                                      command="python s2d2.py -tiles S1B_IW_GRDH_1SDV_20180504T001446_20180504T001511_010764_013ABB_0FBB -platform s1 -s1_sm IW -s1_pt GRD -s1_res H -s1_preprocess 1_S1_FullSceneTest_Orb_Sig0_GM7x7_RD.xml",
                                      job_type="S1_Preprocessing_v1",
                                      parameters=json.dumps({
                                            "tile": "S1B_IW_GRDH_1SDV_20180504T001446_20180504T001511_010764_013ABB_0FBB",
                                            "platform": "s1",
                                            "s1_options": {
                                                "res": "H",
                                                "mode": "IW",
                                                "type": "GRD",
                                                "preprocess_config": "1_S1_FullSceneTest_Orb_Sig0_GM7x7_RD.xml"
                                            }
                                        }),
                                      owner=self.user)

        # self.job2 = Job.objects.create(label="S1 Preprocessing 1_S1_FullSceneTest_Orb_Sig0_GM7x7_RD Carman JOB2",
        #                               command="JOB 2python s2d2.py -tiles S1B_IW_GRDH_1SDV_20180504T001446_20180504T001511_010764_013ABB_0FBB -platform s1 -s1_sm IW -s1_pt GRD -s1_res H -s1_preprocess 1_S1_FullSceneTest_Orb_Sig0_GM7x7_RD.xml",
        #                               job_type="S1_Preprocessing_v1",
        #                               parameters=json.dumps({
        #                                     "tile": "S1B_IW_GRDH_1SDV_20180504T001446_20180504T001511_010764_013ABB_0FBB",
        #                                     "platform": "s1",
        #                                     "s1_options": {
        #                                         "res": "H",
        #                                         "mode": "IW",
        #                                         "type": "GRD",
        #                                         "preprocess_config": "1_S1_FullSceneTest_Orb_Sig0_GM7x7_RD.xml"
        #                                     }
        #                                 }),
        #                               owner=self.user)
        print('JOB ID:')
        print(self.job.id)

        submitted_time = self.job.submitted.replace(tzinfo=None)
        print(submitted_time.isoformat('T') + 'Z')
        # Use to compare with response from API
        self.job_def = {'url': f'http://testserver/jobs/{self.job.id}/', 'id': f'{self.job.id}', 'submitted': f'{submitted_time.isoformat("T") + "Z"}', 'label': 'S1 Preprocessing 1_S1_FullSceneTest_Orb_Sig0_GM7x7_RD Carman', 'command': 'python s2d2.py -tiles S1B_IW_GRDH_1SDV_20180504T001446_20180504T001511_010764_013ABB_0FBB -platform s1 -s1_sm IW -s1_pt GRD -s1_res H -s1_preprocess 1_S1_FullSceneTest_Orb_Sig0_GM7x7_RD.xml', 'job_type': 'S1_Preprocessing_v1', 'parameters': '{"tile": "S1B_IW_GRDH_1SDV_20180504T001446_20180504T001511_010764_013ABB_0FBB", "platform": "s1", "s1_options": {"res": "H", "mode": "IW", "type": "GRD", "preprocess_config": "1_S1_FullSceneTest_Orb_Sig0_GM7x7_RD.xml"}}', 'priority': 'L', 'owner': 'testuser'}


        # 3. Request a job and test that all the logic works


    def test_request_a_job_no_workerid(self):
        """
        Request a job, with no worker_id.
        """

        response = self.client.get(self.getjob_url)

        print(response.data)
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)


    def test_request_a_job(self):
        """
        Request a job, with a worker_id
        """
        # below is an example of doing a GET with a query param
        response = self.client.get(self.getjob_url, {
            'worker_id': self.worker.id
        })

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data, self.job_def)


    def test_request_a_job_job_settings_change(self):
        """
        After the job is requested and assigned, check the job for proper changes.
        """

        response = self.client.get(self.getjob_url, {
            'worker_id': self.worker.id
        })

        time_job_requested = datetime.datetime.now(pytz.utc)

        specific_job_url = reverse('job-detail', kwargs={'pk': response.data['id']})

        response_job_status = self.client.get(specific_job_url)

        print(response_job_status.data)
        print('ready to test status')
        # Check that the job's status is changed to A for assigned
        self.assertEqual(response_job_status.data['status'], 'A')

        # Check that the job's assigned date is close to the job being requested
        # atetime.datetime.strptime(main_timestamp +"Z", "%Y%m%dT%H%M%S.%fZ" )
        time_delta = time_job_requested - datetime.datetime.strptime(response_job_status.data['assigned'],
                                                                     "%Y-%m-%dT%H:%M:%S.%f%z")
        self.assertTrue(time_delta.total_seconds() <= 1)
        print(time_delta.total_seconds())

    def test_worker_has_proper_capabilities(self):
        """
        Before a worker is assigned a job, make sure the worker has the right capability
        """
        # Do 2 requests, 1 with regular worker, 1 with no capability worker

        # response = self.client.get(self.getjob_url, {
        #     'worker_id': self.worker.id
        # })

        # self.assertTrue(response.status_code == status.HTTP_200_OK)

        response_bad = self.client.get(self.getjob_url, {
            'worker_id': self.worker_missing_capabilities.id
        })
        print(response_bad.data)
        self.assertTrue('info' in response_bad.data.keys())

        response_good = self.client.get(self.getjob_url, {
            'worker_id': self.worker.id
        })
        print(response_good.data)
        self.assertTrue('info' not in response_good.data.keys())

    def test_job_updated_with_worker_id(self):
        """
        After a job is assigned to a worker, make sure the worker_id is updated
        on the job.
        """

        response = self.client.get(self.getjob_url, {
            'worker_id': self.worker.id
        })

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        specific_job_url = reverse('job-detail', kwargs={'pk': response.data['id']})

        response_job_status = self.client.get(specific_job_url)
        print(response_job_status.data)
        worker_id_hyperlink = reverse('worker-detail', kwargs={
            'pk': str(self.worker.id)
        })
        self.assertEqual(self.testserver_url_prefix + worker_id_hyperlink, response_job_status.data['worker_id'])


    # def test_register_worker(self):
    #     """
    #     Ensure we properly create a worker when it requests to register
    #     """

    #     response = self.client.post(self.url,
    #                                 self.data_allgood,
    #                                 format='json')

    #     self.assertEqual(response.status_code, status.HTTP_201_CREATED)
    #     self.assertEqual(Worker.objects.count(), 1)
    #     self.assertEqual(Worker.objects.get().worker_name, 'test_worker')

    #     self.assertEqual(response.data['worker_id'], str(Worker.objects.get().id))


    # def test_register_worker_workername_exists(self):
    #     response1 = self.client.post(self.url,
    #                                  self.data_allgood,
    #                                  format='json')

    #     response2 = self.client.post(self.url,
    #                                  self.data_allgood,
    #                                  format='json')

    #     self.assertEqual(response2.status_code, status.HTTP_200_OK)

    #     self.assertEqual(Worker.objects.count(), 1)
    #     self.assertEqual(Worker.objects.get().worker_name, 'test_worker')

    #     self.assertEqual(str(response2.data['worker_id']), str(Worker.objects.get().id))

    #     self.assertEqual(str(response2.data['info']), str('Worker with name test_worker already exists.'))