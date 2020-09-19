#!/usr/bin/env bash

export DJANGO_SETTINGS_MODULE='jobmanager.settings'

celery worker -A jobmanager -l debug -Q downloader --concurrency=1 -n downloader_worker@%h -E --prefetch-multiplier=1
