from django.db import models
import uuid
from django.contrib.postgres.fields import JSONField, ArrayField

from enum import Enum


class JobPriorityChoice(Enum):
    LOW = "3"
    MEDIUM = "2"
    HIGH = "1"


class JobStatusChoice(Enum):
    SUBMITTED = "S"
    ASSIGNED = "A"
    COMPLETED = "C"


# Create your models here.
JOB_PRIORITIES = [("3", "Low"), ("2", "Medium"), ("1", "High")]

JOB_STATUSES = [("S", "Submitted"), ("A", "Assigned"), ("C", "Completed")]


class Worker(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    worker_name = models.TextField()
    system_info = JSONField()
    capabilities = ArrayField(models.CharField(max_length=30))

    paused = models.BooleanField(default=True)

    schedule = JSONField(default=dict)

    created = models.DateTimeField(auto_now_add=True)

    updated = models.DateTimeField(null=True)

    def get_average_time_for_job(self, job_type):
        jobs = Worker.jobset.filter(job_type___like=job_type, success__equals=True)
        total_time = 0
        for job in jobs:
            total_time += (job.assigned - job.completed).total_seconds()

        average_time = total_time / len(jobs)

        return average_time


class Job(models.Model):

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    submitted = models.DateTimeField(auto_now_add=True)
    assigned = models.DateTimeField(null=True, blank=True)
    completed = models.DateTimeField(null=True, blank=True)

    task_id = models.UUIDField(default="00000000-0000-0000-0000-000000000000")

    label = models.CharField(max_length=250, blank=True, default="")
    command = models.TextField()
    job_type = models.TextField()
    parameters = JSONField(default=dict)
    info = JSONField(default=dict)
    priority = models.CharField(
        max_length=1,
        choices=[(tag.value, tag.name) for tag in JobPriorityChoice],
        default="L",
    )
    status = models.CharField(
        max_length=1,
        choices=[(tag.value, tag.name) for tag in JobStatusChoice],
        default="S",
    )
    success = models.BooleanField(default=False)
    result_message = models.TextField(blank=True, null=True)
    aoi_name = models.CharField(max_length=250)
    owner = models.ForeignKey(
        "auth.User", related_name="jobs", on_delete=models.CASCADE
    )

    class Meta:
        ordering = ("submitted",)

    def save(self, *args, **kwargs):  # new
        """
        When the job is created, offload any extra work in the job creation
        to this function, executes when the model is created or saved.
        """
        # change model related fields here (see self.highlighted for example)
        # lexer = get_lexer_by_name(self.language)
        # linenos = 'table' if self.linenos else False
        # options = {'title': self.title} if self.title else {}
        # formatter = HtmlFormatter(style=self.style, linenos=linenos,
        #                           full=True, **options)
        # self.highlighted = highlight(self.code, lexer, formatter)
        super(Job, self).save(*args, **kwargs)

    def __str__(self):
        return f"{self.job_type} {self.submitted.strftime('%Y-%m-%d-%H%M')}"
