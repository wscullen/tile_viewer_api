from django.shortcuts import render

from rest_framework import generics, permissions, renderers

# Create your views here.
from worker.tasks import sen2agri_tasks


class TestL2A(generics.GenericAPIView):
    # queryset = Job.objects.filter(status='S').order_by('-submitted', '-priority')

    def perform_create(self, serializer):
        # serializer.save(owner=self.request.user)
        pass

    def post(self, request, *args, **kwargs):
        print(request)
        print("Trying to POST register a new worker")

        # if len(self.get_queryset()) > 0:
        #     job = self.get_queryset()[0]
        #     s

    def get(self, request, *args, **kwargs):

        imagery_list_example = {
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
        print(request)
        print("Trying to GET to test L2A ")
        task = sen2agri_tasks.start_l2a_job.delay(imagery_list_example, "0")
        print(task)
        result = task.get()
        print(result)
