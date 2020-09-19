from django.urls import reverse

from django.contrib.auth import models

from django.utils import timezone

from rest_framework import status
from rest_framework.test import APITestCase

from api_v1.models import Worker, Job

import datetime
import pytz

import json

class JobEndpointTests(APITestCase):

    def setUp(self):
        self.getjoblist_url = reverse('job-list')

        self.getjob_url = reverse('get-job')
        self.registerworker_url = reverse('register-worker')

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
        self.user2 = models.User.objects.create_user(username='testuser2', password='12345')

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
                                            capabilities=['S2_AtmosCor_v1'],
                                            schedule="{}",
                                            )

        # 2. Create a job
        self.job = Job.objects.create(label="Job 1 -- S1 Preprocessing 1_S1_FullSceneTest_Orb_Sig0_GM7x7_RD Carman",
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
                                      owner=self.user,
                                      priority=2)

        print('JOB ID:')
        print(self.job.id)

        submitted_time = self.job.submitted.replace(tzinfo=None)
        print(submitted_time.isoformat('T') + 'Z')
        # Use to compare with response from API
        self.job_def = {'url': f'http://testserver/jobs/{self.job.id}/', 'id': f'{self.job.id}', 'submitted': f'{submitted_time.isoformat("T") + "Z"}', 'label': 'S1 Preprocessing 1_S1_FullSceneTest_Orb_Sig0_GM7x7_RD Carman', 'command': 'python s2d2.py -tiles S1B_IW_GRDH_1SDV_20180504T001446_20180504T001511_010764_013ABB_0FBB -platform s1 -s1_sm IW -s1_pt GRD -s1_res H -s1_preprocess 1_S1_FullSceneTest_Orb_Sig0_GM7x7_RD.xml', 'job_type': 'S1_Preprocessing_v1', 'parameters': '{"tile": "S1B_IW_GRDH_1SDV_20180504T001446_20180504T001511_010764_013ABB_0FBB", "platform": "s1", "s1_options": {"res": "H", "mode": "IW", "type": "GRD", "preprocess_config": "1_S1_FullSceneTest_Orb_Sig0_GM7x7_RD.xml"}}', 'priority': 'L', 'owner': 'testuser'}

    def create_various_jobs(self):
        job2 = Job.objects.create(label="Job 2 -- S1 Preprocessing 1_S1_FullSceneTest_Orb_Sig0_GM7x7_RD Carman JOB2",
                                      command="JOB 2python s2d2.py -tiles S1B_IW_GRDH_1SDV_20180504T001446_20180504T001511_010764_013ABB_0FBB -platform s1 -s1_sm IW -s1_pt GRD -s1_res H -s1_preprocess 1_S1_FullSceneTest_Orb_Sig0_GM7x7_RD.xml",
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
                                      success=False,
                                      status='S',
                                      owner=self.user,
                                      priority=1)

        job3 = Job.objects.create(label="Job 3 -- S1 Preprocessing 1_S1_FullSceneTest_Orb_Sig0_GM7x7_RD Carman JOB2",
                                      command="JOB 2python s2d2.py -tiles S1B_IW_GRDH_1SDV_20180504T001446_20180504T001511_010764_013ABB_0FBB -platform s1 -s1_sm IW -s1_pt GRD -s1_res H -s1_preprocess 1_S1_FullSceneTest_Orb_Sig0_GM7x7_RD.xml",
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
                                      success=True,
                                      result_message='Job completed successfully',
                                      status='C',
                                      completed=datetime.datetime.utcnow(),
                                      worker_id=self.worker_missing_capabilities,
                                      owner=self.user,
                                      priority=1)

        job4 = Job.objects.create(label="Job 4 -- S2 AtmosCor Standard",
                                      command="python s2d2.py -tiles S2A_MSIL1C_20180721T152631_N0206_R068_T20UMV_20180721T202434 -platform s2 -ac 10 -c 100 -b -cli",
                                      job_type="S2_AtmosCor_v1",
                                      parameters=json.dumps({
                                            "tile": "S2A_MSIL1C_20180721T152631_N0206_R068_T20UMV_20180721T202434",
                                            "platform": "s2",
                                            "s2_options": {
                                            }
                                        }),
                                      success=False,
                                      status='A',
                                      assigned=datetime.datetime.utcnow(),
                                      worker_id=self.worker,
                                      owner=self.user2,
                                      priority=3)

        return [job2, job3, job4]

    def test_job_complete_update_no_auth_no_workerid(self):
        """
        Worker requests job, and updates job to be complete.

        Should use API endpoint to:
        1. Set job status to 'C'
        2. Set completed to datetime.now()
        3. Result message has info or log regarding how the task was completed.
        4. success is set to True (or left False)
        """

        response = self.client.get(self.getjob_url, {
            'worker_id': self.worker.id
        })

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # call the jobs/job_id endpoint to mark the job as complete
        specific_job_url = reverse('job-detail', kwargs={'pk': response.data['id']})

        data = {
            'status': 'C',
            'success': True,
            'result_message': 'Job was completed successfully'
        }

        response = self.client.post(specific_job_url, data, format='json')

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_job_complete_update_auth(self):
        """
        Worker requests job, and updates job to be complete.

        Should use API endpoint to:
        1. Set job status to 'C'
        2. Set completed to datetime.now()
        3. Result message has info or log regarding how the task was completed.
        4. success is set to True (or left False)
        """

        response = self.client.get(self.getjob_url, {
            'worker_id': self.worker.id
        })

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # call the jobs/job_id endpoint to mark the job as complete
        specific_job_url = reverse('job-detail', kwargs={'pk': response.data['id']})

        data = {
            'status': 'C',
            'success': True,
            'result_message': 'Job was completed successfully'
        }

        self.client.force_authenticate(user='testuser')

        time_job_completed = timezone.now()

        response = self.client.post(specific_job_url, data, format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # Test for job complete date
        # verify values of the job
        job_response = self.client.get(specific_job_url, format='json')

        self.assertEqual(job_response.data['result_message'], data['result_message'])
        self.assertEqual(job_response.data['success'], data['success'])
        self.assertEqual(job_response.data['status'], data['status'])

        # Check that the job's assigned date is close to the job being requested
        # atetime.datetime.strptime(main_timestamp +"Z", "%Y%m%dT%H%M%S.%fZ" )
        time_delta = time_job_completed - datetime.datetime.strptime(job_response.data['completed'],
                                                                     "%Y-%m-%dT%H:%M:%S.%f%z")
        self.assertTrue(time_delta.total_seconds() <= 1)


    def test_job_complete_update_with_workerid(self):
        """
        Worker requests job, and updates job to be complete.

        Should use API endpoint to:
        1. Set job status to 'C'
        2. Set completed to datetime.now()
        3. Result message has info or log regarding how the task was completed.
        4. success is set to True (or left False)
        """

        response = self.client.get(self.getjob_url, {
            'worker_id': self.worker.id
        })

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # call the jobs/job_id endpoint to mark the job as complete
        specific_job_url = reverse('job-detail', kwargs={'pk': response.data['id']})

        data = {
            'status': 'C',
            'success': True,
            'result_message': 'Job was completed successfully',
            'worker_id': self.worker.id
        }

        time_job_completed = timezone.now()
        response2 = self.client.post(specific_job_url, data=data, format='json')

        self.assertEqual(response2.status_code, status.HTTP_200_OK)

        # verify values of the job
        job_response = self.client.get(specific_job_url, format='json')

        self.assertEqual(job_response.data['result_message'], data['result_message'])
        self.assertEqual(job_response.data['success'], data['success'])
        self.assertEqual(job_response.data['status'], data['status'])

         # Test for job complete date
        # verify values of the job
        job_response = self.client.get(specific_job_url, format='json')

        self.assertEqual(job_response.data['result_message'], data['result_message'])
        self.assertEqual(job_response.data['success'], data['success'])
        self.assertEqual(job_response.data['status'], data['status'])

        # Check that the job's assigned date is close to the job being requested
        # atetime.datetime.strptime(main_timestamp +"Z", "%Y%m%dT%H%M%S.%fZ" )
        time_delta = time_job_completed - datetime.datetime.strptime(job_response.data['completed'],
                                                                     "%Y-%m-%dT%H:%M:%S.%f%z")
        self.assertTrue(time_delta.total_seconds() <= 1)

    def test_job_complete_update_with_workerid_missing_values(self):
        """
        Worker requests job, and updates job to be complete.

        Should use API endpoint to:
        1. Set job status to 'C'
        2. Set completed to datetime.now()
        3. Result message has info or log regarding how the task was completed.
        4. success is set to True (or left False)
        """

        response = self.client.get(self.getjob_url, {
            'worker_id': self.worker.id
        })

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # call the jobs/job_id endpoint to mark the job as complete
        specific_job_url = reverse('job-detail', kwargs={'pk': response.data['id']})

        data = {
            'status': 'C',
            'worker_id': self.worker.id
        }

        response2 = self.client.post(specific_job_url, data=data, format='json')

        self.assertEqual(response2.status_code, status.HTTP_400_BAD_REQUEST)

        job_response = self.client.get(specific_job_url, format='json')

        self.assertEqual(job_response.data['result_message'], None)
        self.assertEqual(job_response.data['success'], False)
        self.assertEqual(job_response.data['status'], 'A')


    def test_job_list_filtering_by_status(self):
        """
        Test filtering jobs by status, success, worker_id, owner, job_type
        """

        jobs = self.create_various_jobs()

        # Test filtering by status
        response = self.client.get(self.getjoblist_url, {
            'status': 'C'
        })

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]['status'], 'C')
        self.assertEqual(response.data[0]['id'], str(jobs[1].id))

    def test_job_list_filtering_by_success(self):
        """
        Test filtering jobs by status, success, worker_id, owner, job_type
        """

        jobs = self.create_various_jobs()

        # Test filtering by status
        response = self.client.get(self.getjoblist_url, {
            'success': False
        })

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 3)
        self.assertEqual(response.data[0]['success'], False)
        self.assertEqual(response.data[1]['success'], False)

        self.assertTrue(response.data[0]['id'] in [str(self.job.id),
                                                    str(jobs[0].id),
                                                    str(jobs[2].id),
                                                    ])

        self.assertTrue(response.data[1]['id'] in [str(self.job.id),
                                                    str(jobs[0].id),
                                                    str(jobs[2].id),
                                                    ])

        self.assertTrue(response.data[2]['id'] in [str(self.job.id),
                                                    str(jobs[0].id),
                                                    str(jobs[2].id),
                                                    ])

         # Test filtering by status
        response = self.client.get(self.getjoblist_url, {
            'success': True
        })

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)

        self.assertTrue(response.data[0]['id'] in [str(jobs[1].id)])

    def test_job_list_filtering_by_workerid(self):
        """
        Test filtering jobs by status, success, worker_id, owner, job_type
        """

        jobs = self.create_various_jobs()

        # Test filtering by status
        response = self.client.get(self.getjoblist_url, {
            'worker_id': str(self.worker.id)
        })

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)

        self.assertTrue(response.data[0]['id'] in [str(jobs[2].id)])
        self.assertTrue(response.data[0]['worker_id'], str(self.worker.id))

         # Test filtering by status
        response = self.client.get(self.getjoblist_url, {
            'worker_id': str(self.worker_missing_capabilities.id)
        })

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)

        self.assertTrue(response.data[0]['id'] in [str(jobs[1].id)])

    def test_job_list_filtering_by_owner(self):
        """
        Test filtering jobs by status, success, worker_id, owner, job_type
        """

        jobs = self.create_various_jobs()

        # Test filtering by status
        response = self.client.get(self.getjoblist_url, {
            'owner': str(self.user.id)
        })

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 3)

        self.assertTrue(response.data[0]['id'] in [str(jobs[0].id),
                                                   str(jobs[1].id),
                                                   str(self.job.id)])
        self.assertTrue(response.data[1]['id'] in [str(jobs[0].id),
                                                   str(jobs[1].id),
                                                   str(self.job.id)])
        self.assertTrue(response.data[2]['id'] in [str(jobs[0].id),
                                                   str(jobs[1].id),
                                                   str(self.job.id)])

        self.assertTrue(response.data[0]['owner'], str(self.user.id))

        # Test filtering by status
        response = self.client.get(self.getjoblist_url, {
            'owner': str(self.user2.id)
        })

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)

        self.assertTrue(response.data[0]['id'] in [str(jobs[2].id)])
        self.assertTrue(response.data[0]['owner'], str(self.user2.id))

    def test_job_list_filtering_by_jobtype(self):
        """
        Test filtering jobs by status, success, worker_id, owner, job_type
        """

        jobs = self.create_various_jobs()

        # Test filtering by status
        response = self.client.get(self.getjoblist_url, {
            'job_type': 'S1_Preprocessing_v1'
        })

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 3)

        self.assertTrue(response.data[0]['id'] in [str(jobs[0].id),
                                                   str(jobs[1].id),
                                                   str(self.job.id)])
        self.assertTrue(response.data[1]['id'] in [str(jobs[0].id),
                                                   str(jobs[1].id),
                                                   str(self.job.id)])
        self.assertTrue(response.data[2]['id'] in [str(jobs[0].id),
                                                   str(jobs[1].id),
                                                   str(self.job.id)])

        self.assertTrue(response.data[0]['job_type'], str(self.job.job_type))

        # Test filtering by status
        response = self.client.get(self.getjoblist_url, {
            'job_type': 'S2_AtmosCor_v1'
        })

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)

        self.assertTrue(response.data[0]['id'] in [str(jobs[2].id)])
        self.assertTrue(response.data[0]['owner'], str(self.user2.id))

    def test_job_list_ordering(self):
        """
        Test that the returned jobs are ordered properly,
        results can be ordered by priority, date added, assigned, and completed
        """

        jobs = self.create_various_jobs()

        # Test filtering by status
        response = self.client.get(self.getjoblist_url, {
            'ordering': 'priority'
        })

        job_response_title_list = [item['label'][:5] for item in response.data]

        self.assertEqual(job_response_title_list, [
            'Job 2',
            'Job 3',
            'Job 1',
            'Job 4'
        ])

         # Test filtering by status
        response = self.client.get(self.getjoblist_url, {
            'ordering': '-submitted'
        })

        job_response_title_list = [item['label'][:5] for item in response.data]

        self.assertEqual(job_response_title_list, [
            'Job 4',
            'Job 3',
            'Job 2',
            'Job 1'
        ])

        # Test filtering by status
        response = self.client.get(self.getjoblist_url, {
            'ordering': 'completed'
        })

        job_response_title_list = [item['label'][:5] for item in response.data]

        self.assertEqual(job_response_title_list, [
            'Job 3',
            'Job 1',
            'Job 2',
            'Job 4'
        ])

        # Test filtering by status
        response = self.client.get(self.getjoblist_url, {
            'ordering': 'assigned'
        })

        job_response_title_list = [item['label'][:5] for item in response.data]

        self.assertEqual(job_response_title_list, [
            'Job 4',
            'Job 1',
            'Job 2',
            'Job 3'
        ])