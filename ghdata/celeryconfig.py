#!/usr/bin/env python
# -*- coding: utf-8 -*-

# from datetime import timedelta
from celery.schedules import crontab
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
        # 'schedule': timedelta(minutes=1)
        'schedule': crontab(minute=30)
    },
    'fetch-timeline-data': {
        'task': 'ghdata.tasks.fetch_timeline',
        'args': (2012, 3, 1, 8),
        # 'schedule': timedelta(minutes=1)
        'schedule': crontab(hour=20, minute=0)
    },
    'country-rank': {
        'task': 'ghdata.tasks.country_rank',
        'schedule': crontab(day_of_month=2, hour=0, minute=0)
    },
    'city-rank': {
        'task': 'ghdata.tasks.city_rank',
        'schedule': crontab(day_of_month=2, hour=2, minute=0)
    }
}

CELERY_TIMEZONE = 'Asia/Shanghai'

CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'

BROKER_TRANSPORT_OPTIONS = {
    'visibility_timeout': 3600 * 24
}

CELERY_ROUTES = {
    'ghdata.tasks.update_user': {'queue': 'github'},
    'ghdata.tasks.update_location': {'queue': 'geo'},
    'ghdata.tasks.fetch_worker': {'queue': 'fetch'},
    'ghdata.tasks.country_rank': {'queue': 'stats'},
    'ghdata.tasks.city_rank': {'queue': 'stats'},
    'ghdata.tasks.update_all_users': {'queue': 'celery'},
    'ghdata.tasks.fetch_timeline': {'queue': 'celery'}
}
