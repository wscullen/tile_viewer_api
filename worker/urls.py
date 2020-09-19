# api_v1/urls

from django.urls import path
from rest_framework.urlpatterns import format_suffix_patterns
from . import views

from rest_framework.schemas import get_schema_view

schema_view = get_schema_view(title="Job Manager API v1")

urlpatterns = [path("test_l2a/", views.TestL2A.as_view(), name="test-l2a")]

urlpatterns = format_suffix_patterns(urlpatterns)
