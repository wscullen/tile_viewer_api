#!/usr/bin/env bash

export DJANGO_SETTINGS_MODULE='jobmanager.integration_testing'

celery purge -A jobmanager -f && celery worker -A jobmanager -l info -Q sen2agri
