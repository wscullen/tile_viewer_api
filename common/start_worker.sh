#!/usr/bin/env bash

export DJANGO_SETTINGS_MODULE='jobmanager.settings'

python3.7 manage.py shell < common/simple_worker.py
