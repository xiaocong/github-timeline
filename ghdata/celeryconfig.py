#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import timedelta
# from celery.schedules import crontab
from . import config

# Included Taskes
CELERY_INCLUDE = ['ghdata.tasks']
# Task Broker
BROKER_URL = config.REDIS_URI
# Task Result backend
CELERY_RESULT_BACKEND = config.REDIS_URI
CELERY_TASK_RESULT_EXPIRES = 60

# pool and threads
# CELERYD_POOL = "gevent"
CELERYD_CONCURRENCY = 50
CELERYD_PREFETCH_MULTIPLIER = 1

# Scheduled tasks
CELERYBEAT_SCHEDULE = {
    'crawl-user-info': {
        'task': 'ghdata.tasks.update_all_users',
        'schedule': timedelta(seconds=60)
    },
    'fetch-timeline-data': {
        'task': 'ghdata.tasks.fetch_timeline',
        'schedule': timedelta(seconds=60)
    }
}

CELERY_TIMEZONE = 'Asia/Shanghai'

CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'

BROKER_TRANSPORT_OPTIONS = {
    'visibility_timeout': 3600 * 3
}

CELERY_ROUTES = {
    'ghdata.tasks.update_user': {'queue': 'github'},
    'ghdata.tasks.update_location': {'queue': 'geo'},
    'ghdata.tasks.update_all_users': {'queue': 'celery'},
    'ghdata.tasks.fetch_timeline': {'queue': 'celery'},
    'ghdata.tasks.fetch_worker': {'queue': 'celery'}
}
