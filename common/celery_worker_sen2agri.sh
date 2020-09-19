#!/usr/bin/env bash

export DJANGO_SETTINGS_MODULE='jobmanager.settings'

celery worker -A jobmanager -l info -Q sen2agri --concurrency=1 -n sen2agri_worker@%h -E
