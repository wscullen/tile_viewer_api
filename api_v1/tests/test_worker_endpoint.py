from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase
from api_v1.models import Worker


class WorkerTests(APITestCase):

    def setUp(self):
        self.url = reverse('register-worker')
        self.data_noservercode = {
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
            ]
        }
        self.data_allgood = {
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

    def test_register_worker_no_servercode(self):
        """
        Ensure we properly create a worker when it requests to register
        """

        response = self.client.post(self.url,
                                    self.data_noservercode,
                                    format='json')

        print(response.data)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(Worker.objects.count(), 0)


    def test_register_worker(self):
        """
        Ensure we properly create a worker when it requests to register
        """

        response = self.client.post(self.url,
                                    self.data_allgood,
                                    format='json')

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(Worker.objects.count(), 1)
        self.assertEqual(Worker.objects.get().worker_name, 'test_worker')

        self.assertEqual(response.data['worker_id'], str(Worker.objects.get().id))


    def test_register_worker_workername_exists(self):
        response1 = self.client.post(self.url,
                                     self.data_allgood,
                                     format='json')

        response2 = self.client.post(self.url,
                                     self.data_allgood,
                                     format='json')

        self.assertEqual(response2.status_code, status.HTTP_200_OK)

        self.assertEqual(Worker.objects.count(), 1)
        self.assertEqual(Worker.objects.get().worker_name, 'test_worker')

        self.assertEqual(str(response2.data['worker_id']), str(Worker.objects.get().id))

        self.assertEqual(str(response2.data['info']), str('Worker with name test_worker already exists.'))