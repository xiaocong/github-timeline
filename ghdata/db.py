#!/usr/bin/env python
# -*- coding: utf-8 -*-

import redis as _redis
import os
from pymongo import MongoClient

from .config import *

pool = _redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
mongo_client = MongoClient(MONGODB_URI)


def redis():
    return _redis.Redis(connection_pool=pool)


def pipe():
    return redis().pipeline()


def format_key(key):
    return "%s:%s" % (os.environ.get('PREFIX', 'gtl'), key)


def mongodb():
    return mongo_client.github
