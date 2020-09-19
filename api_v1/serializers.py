# api_v1/serializers

from rest_framework import serializers
from django.contrib.auth.models import User

from .models import Job, Worker, JOB_PRIORITIES, JOB_STATUSES

# Required serializer validation
def required(value):
    print(value)
    if value is None:
        raise serializers.ValidationError("This field is required")


class JobSerializer(serializers.HyperlinkedModelSerializer):

    # owner = serializers.ReadOnlyField(source='owner.username')
    owner = serializers.HyperlinkedRelatedField(view_name="user-detail", read_only=True)

    class Meta:
        model = Job
        fields = (
            "url",
            "id",
            "aoi_name",
            "submitted",
            "assigned",
            "completed",
            "label",
            "command",
            "job_type",
            "task_id",
            "parameters",
            "info",
            "priority",
            "status",
            "success",
            "result_message",
            "owner",
        )


class JobStatusSerializer(serializers.ModelSerializer):
    owner = serializers.ReadOnlyField(source="owner.username")

    class Meta:
        model = Job
        fields = (
            "url",
            "id",
            "aoi_name",
            "submitted",
            "assigned",
            "completed",
            "label",
            "command",
            "job_type",
            "task_id",
            "parameters",
            "info",
            "priority",
            "status",
            "success",
            "result_message",
            "owner",
        )


class JobCreateSerializer(serializers.HyperlinkedModelSerializer):

    owner = serializers.ReadOnlyField(source="owner.username")

    class Meta:
        model = Job
        fields = (
            "url",
            "id",
            "aoi_name",
            "submitted",
            "label",
            "command",
            "job_type",
            "parameters",
            "info",
            "priority",
            "task_id",
            "owner",
        )


class WorkerJobCompleteSerializer(serializers.ModelSerializer):

    status = serializers.CharField(max_length=1, validators=[required])
    success = serializers.BooleanField(validators=[required])
    result_message = serializers.CharField(validators=[required])

    def validate(self, data):
        for required in ["success", "status", "result_message"]:
            if required not in data.keys():
                raise serializers.ValidationError

        return data

    class Meta:
        model = Job
        fields = ("worker_id", "success", "status", "result_message")


class WorkerSerializer(serializers.HyperlinkedModelSerializer):

    # owner = serializers.ReadOnlyField(source='owner.username')
    # owner = serializers.HyperlinkedRelatedField(view_name='user-detail',
    #                                            read_only=True)

    class Meta:
        model = Worker
        fields = (
            "id",
            "worker_name",
            "system_info",
            "capabilities",
            "paused",
            "schedule",
            "created",
            "updated",
        )


class WorkerCreateSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = Worker
        fields = (
            "id",
            "worker_name",
            "system_info",
            "capabilities",
            "paused",
            "schedule",
        )


class UserSerializer(serializers.HyperlinkedModelSerializer):
    jobs = serializers.HyperlinkedRelatedField(
        many=True, view_name="job-detail", read_only=True
    )

    class Meta:
        model = User
        fields = ("url", "id", "username", "jobs", "email")
