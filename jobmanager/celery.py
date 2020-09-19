from __future__ import absolute_import, unicode_literals
import os
from celery import Celery
from celery.schedules import crontab


import logging

# set the default Django settings module for the 'celery' program.
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "jobmanager.settings")

app = Celery(
    "jobmanager",
    include=[
        "worker.tasks.sen2agri_tasks",
        "worker.tasks.sen2agri_tasks_l3a",
        "worker.tasks.sen2agri_tasks_l3b",
        "worker.tasks.download_s2",
        "worker.tasks.download_l8",
        "worker.tasks",
        "api_v1.tasks",
        "s2d2_app.tasks",
    ],
)

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object("django.conf:settings", namespace="CELERY")

# Load task modules from all registered Django app configs.
try:
    app.autodiscover_tasks()
except:
    print("lol please")


@app.task(bind=True)
def debug_task(self, num1, num2):
    print("Request: {0!r}".format(self.request))
    print(num1)
    print(num2)

    return num1 + num2


@app.task(bind=True)
def add_numbers2(self, x, y):
    print(x)
    print(y)
    result = x + y
    print(result)
    return result


# from worker.tasks.sen2agri_tasks import check_for_l2a_jobs

# from api_v1.tasks import debug_shared_task


# from api_v1.tasks import check_jobs


# Setup periodic tasks
@app.on_after_finalize.connect
def setup_periodic_tasks(sender, **kwargs):

    # sender.add_periodic_task(
    #     10.0,
    #     debug_task.s(4, 4)
    # )

    # sender.add_periodic_task(
    #     10.0,
    #     debug_shared_task.s('poppycock')
    # )

    # logging.info(jobmanager.bulk_submit_fetch_l8)

    # sender.add_periodic_task(10.0, check_for_l2a_jobs.s())
    from api_v1.tasks import bulk_submit_l8, bulk_fetch_l8

    sender.add_periodic_task(
        # # crontab(hour='*/1'),
        # crontab(minute='*/1'),
        # crontab(minute="0,10,20,30,40,50"),
        30.0,
        bulk_submit_l8.s(),
    )

    # sender.add_periodic_task(
    #     # # crontab(hour='*/1'),
    #     # crontab(minute='*/1'),
    #     # crontab(minute="5,15,25,35,45,55"),
    #     30.0,
    #     bulk_fetch_l8.s(),
    # )

    from api_v1.tasks import check_jobs, check_offline_queues_for_all_users

    sender.add_periodic_task((60.0 * 5.0), check_offline_queues_for_all_users.s())

    sender.add_periodic_task(30.0, check_jobs.s())

    # from worker.tasks.fuck import check_for_l2a_jobs

    # sender.add_periodic_task(30.0, check_for_l2a_jobs.s())

    # sender.add_periodic_task(
    #     # # crontab(hour='*/1'),
    #     # crontab(minute='*/1'),
    #     10.0,
    #     debug_task.s(),
    # )

    # sender.add_periodic_task(
    #     crontab(minute='58-59'),
    #     jobmanager.tasks.test.s('here I am! Dont run in the worker!'),
    # )

from celery.signals import worker_shutting_down
# from celery.task.control import  inspect
# from celery import current_app

@worker_shutting_down.connect
def worker_shutting_down_handler(sig, how, exitcode, ** kwargs):
    print(kwargs)
    print(f'worker_shutting_down({sig}, {how}, {exitcode})')
    # i = inspect()
    # print(i.registered_tasks())
    # tasks = list(sorted(name for name in current_app.tasks
    #                         if not name.startswith('celery.')))
    # print(tasks)
    from celery import current_app

    i = current_app.control.inspect()
    print(kwargs['sender'])
    # i = app.control.inspect()
    print(i.registered())
    print(i.active())
