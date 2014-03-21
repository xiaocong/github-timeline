#!/usr/bin/env python
# -*- coding: utf-8 -*-

import redis as _redis
import os
from pymongo import MongoClient

redis_host = os.environ.get('REDIS_SERVER', 'localhost')
redis_port = int(os.environ.get('REDIS_PORT', 6379))
redis_db = int(os.environ.get('REDIS_DB', 1))
pool = _redis.ConnectionPool(host=redis_host, port=redis_port, db=redis_db)

mongo_url = os.environ.get('MONGO_URL', 'mongodb://localhost:27017/')
mongo_client = MongoClient(mongo_url)


def redis():
    return _redis.Redis(connection_pool=pool)


def pipe():
    return redis().pipeline()


def format_key(key):
    return "%s:%s" % (os.environ.get('PREFIX', 'gtl'), key)


def mongodb():
    return mongo_client.github
