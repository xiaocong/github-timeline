#!/usr/bin/env python
# -*- coding: utf-8 -*-

import redis as _redis
import os
from pymongo import MongoClient

pool = _redis.ConnectionPool(host='localhost', port=6379, db=1)
mongo_client = MongoClient()


def redis():
    return _redis.Redis(connection_pool=pool)


def pipe():
    return redis().pipeline()


def format_key(key):
    return "%s:%s" % (os.environ.get('PREFIX', 'gtl'), key)


def mongodb():
    return mongo_client.github
