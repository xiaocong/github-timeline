#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import

from .worker import worker as w, logger

import gevent
import gevent.queue
import requests
import time
from datetime import datetime, timedelta
import functools

from .config import GITHUB_CRENDENTIALS
from .db import mongodb, redis, format_key as _format
from .geo import geo_info
from .fetch import fetch_one, events_process, file_process

ghapi_url = "https://api.github.com/users/{username}"
geoname_url = "http://api.geonames.org/search"


def concurrency(n):
    def wrapper(fn):
        @functools.wraps(fn)
        def wrap(*args, **kwargs):
            r = redis()
            key = _format('func:concurrency:%s' % fn.__name__)
            try:
                if r.incr(key) > n:
                    logger.info("No more tasks for %s..." % fn.__name__)
                else:
                    return fn(*args, **kwargs)
            finally:
                r.decr(key)
        return wrap
    return wrapper


@w.task
def update_user(username):
    logger.info("Updating %s" % username)
    while not _update_user(username):
        logger.info("Holding github user task for 10 minutes.")
        time.sleep(10 * 60)


def _update_user(username):
    username = username.lower()
    users = mongodb().users_stats
    user = users.find_one({"_id": username}, {"info": 1})

    successful = True
    if user:
        info = user.get('info', {})
        etag = info.get("etag", None)
        location = info.get("location", None)

        r = None
        try:
            # Work out the authentication headers.
            auth = {}
            client_id, client_secret = GITHUB_CRENDENTIALS.split(":")
            if client_id is not None and client_secret is not None:
                auth["client_id"] = client_id
                auth["client_secret"] = client_secret

            # Perform a conditional fetch on the database.
            headers = {}
            if etag is not None:
                headers = {"If-None-Match": etag}
            r = requests.get(ghapi_url.format(username=username), params=auth,
                             headers=headers)
            code = r.status_code
            if code == requests.codes.ok:
                data = r.json()
                data['etag'] = r.headers["ETag"]
                users.update({"_id": username},
                             {"$set": {"info": data}},
                             upsert=True)
                location = data.get('location', None)
            elif code == 403:
                logger.info("*** Limitation reached.")
                successful = False
        finally:
            if r:
                r.close()
        if location not in [None, ""]:
            update_location.delay(location)
    return successful


@w.task
def update_location(location):
    if location in [None, ""]:
        return

    logger.info("Retrieving location %s" % location)
    location = location.lower()
    locations = mongodb().locations
    loc = locations.find_one({"_id": location})
    if loc:
        return  # return if the loc is already in the db
    loc = {"_id": location}
    info = geo_info(location)
    if info:
        loc.update(info)
    locations.update({"_id": location}, loc, True)


@w.task(ignore_result=True)
@concurrency(1)
def update_all_users():

    def _worker(q):
        for name in q:
            try:
                update_user.s(name).delay().get()
            except Exception as e:
                logger.error(e)

    index, count, r = 0, 100, redis()
    q = gevent.queue.Queue(count)
    workers = [gevent.spawn(_worker, q) for i in range(count)]
    while True:
        names = r.zrevrange(_format("user"), index, index + count)
        for name in names:
            q.put(name)
        if len(names) < count:
            break
        index += count
        logger.info("Updated %d users." % index)
    for i in range(len(workers)):
        q.put(StopIteration)
    gevent.joinall(workers)


@w.task(ignore_result=True)
@concurrency(1)
def fetch_timeline():
    def _worker(q):
        # worker process.
        for year, month, day, hour in q:
            fetch_worker.s(year, month, day, hour).delay().get()
    q = gevent.queue.Queue(8)
    workers = [gevent.spawn(_worker, q) for i in range(4)]
    since = datetime(2012, 3, 1)
    while since < datetime.today() - timedelta(days=1):
        q.put([since.year, since.month, since.day, since.hour])
        since += timedelta(hours=1)
    [q.put(StopIteration) for w in workers]
    gevent.joinall(workers)


@w.task
def fetch_worker(year, month, day, hour):
    try:
        file_process(fetch_one(year, month, day, hour), events_process)
    except Exception as e:
        logger.error("Error during processing %d-%d-%d %d hr: %s" % (year, month, day, hour, e))
