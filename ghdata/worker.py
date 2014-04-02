#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import

from celery import Celery
from celery.utils.log import get_task_logger

from . import celeryconfig

logger = get_task_logger(__name__)

worker = Celery('ghdata.worker')
# Optional configuration, see the application user guide.
worker.config_from_object(celeryconfig)

if __name__ == '__main__':
    worker.start()
