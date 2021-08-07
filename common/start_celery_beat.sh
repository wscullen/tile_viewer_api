#!/usr/bin/env bash

export DJANGO_SETTINGS_MODULE='jobmanager.settings'

celery -A jobmanager beat -l debug --scheduler django_celery_beat.schedulers:DatabaseScheduler
