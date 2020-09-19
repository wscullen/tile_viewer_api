# api_v1/urls

from django.urls import path
from rest_framework.urlpatterns import format_suffix_patterns
from . import views

from rest_framework.schemas import get_schema_view

schema_view = get_schema_view(title="Job Manager API v1")

urlpatterns = [
    path("schema/", schema_view),
    # path("job/", views.GetJob.as_view(), name="get-job"),
    path("jobs/", views.JobList.as_view(), name="job-list"),
    path("jobs/<uuid:pk>/", views.JobDetail.as_view(), name="job-detail"),
    path("jobs/<uuid:pk>/retry", views.JobRetry.as_view(), name="job-retry"),
    path("worker/", views.RegisterWorker.as_view(), name="register-worker"),
    path("workers/", views.WorkerList.as_view(), name="worker-list"),
    path("workers/<uuid:pk>/", views.WorkerDetail.as_view(), name="worker-detail"),
    path("users/", views.UserList.as_view(), name="user-list"),
    path("users/<int:pk>/", views.UserDetail.as_view(), name="user-detail"),
    path("", views.api_root),
    path("imagerystatus/", views.ImageryStatus.as_view(), name="imagery-status"),
    path("redistest/", views.RedisGetSetTest.as_view(), name="redis-test"),
]

urlpatterns += [path("generate_csrf/", views.CSRFGeneratorView.as_view())]
urlpatterns += [path("add/", views.TestEndPoint.as_view())]
urlpatterns += [path("version/", views.Version.as_view())]


urlpatterns = format_suffix_patterns(urlpatterns)
