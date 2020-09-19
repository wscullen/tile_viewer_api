from django.test import TestCase

# Create your tests here.
from api_v1.models import Worker


class WorkerModelTestCase(TestCase):
    def setUp(self):
        # Worker.objects.create(worker_name='test_worker1')
        pass

    def test_worker_registration(self):
        """When a worker has the proper passcode it can register successfully"""
        print('Wow')