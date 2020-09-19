#!/usr/bin/env bash

export DJANGO_SETTINGS_MODULE='jobmanager.settings'

celery worker -A jobmanager -Q periodic --concurrency=1
