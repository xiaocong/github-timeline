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
from collections import defaultdict
from translate import Translator

from .config import GITHUB_CRENDENTIALS
from .db import mongodb, redis, format_key as _format
from .geo import geo_info
from .fetch import fetch_one, events_process, file_process, events_process_lang_contrib

ghapi_url = "https://api.github.com/users/{username}"
geoname_url = "http://api.geonames.org/search"


def concurrency(n):
    '''no more than n processes running.'''
    def wrapper(fn):
        @functools.wraps(fn)
        def wrap(*args, **kwargs):
            r = redis()
            key = _format('worker:concurrency')
            try:
                if r.hincrby(key, fn.__name__, 1) > n:
                    logger.info("No more tasks for %s..." % fn.__name__)
                else:
                    return fn(*args, **kwargs)
            except:
                pass
            finally:
                r.hincrby(key, fn.__name__, -1)
        return wrap
    return wrapper


@w.task
def update_user(username):
    '''update user's info from github'''
    logger.info("Updating %s" % username)
    while not _update_user(username):
        logger.info("Holding github user task for 10 minutes.")
        time.sleep(10 * 60)


def _update_user(username):
    username = username.lower()
    users = mongodb().users_stats
    user = users.find_one({"_id": username}, {"info": 1})

    successful = True
    info = user.get('info', {}) if user else {}
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
        headers = {"If-None-Match": etag} if etag else {}
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
    except:
        pass
    finally:
        if r:
            r.close()
    if location not in [None, ""]:
        if not mongodb().locations.find_one({"_id": location.lower()}):
            update_location.delay(location)
    return successful


@w.task
def update_location(location):
    '''get location data(contry, city...) from location name'''
    if location in [None, ""]:
        return

    logger.info("Retrieving location %s" % location)
    location = location.lower()
    locations = mongodb().locations
    if locations.find_one({"_id": location}):
        return  # return if the loc is already in the db
    loc = {"_id": location}
    loc.update(geo_info(location) or {})
    locations.update({"_id": location}, loc, True)


@w.task(ignore_result=True)
@concurrency(1)
def update_all_users():
    '''Traverse all users and retrieve its info from github.'''
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
def fetch_timeline(year=2012, month=3, day=1, threads=4):
    '''worker process to go through all timeline data since 2012/3/1.'''
    def _worker(q):
        # worker process.
        for year, month, day, hour in q:
            fetch_worker.s(year, month, day, hour).delay().get()
    q = gevent.queue.Queue(8)
    workers = [gevent.spawn(_worker, q) for i in range(threads)]
    since = datetime(year, month, day)
    while since < datetime.today() - timedelta(days=1):
        q.put([since.year, since.month, since.day, since.hour])
        since += timedelta(hours=1)
    [q.put(StopIteration) for w in workers]
    gevent.joinall(workers)


@w.task
def fetch_worker(year, month, day, hour):
    '''fetch one hour's timeline data and save it to db.'''
    try:
        file_process(fetch_one(year, month, day, hour), [events_process, events_process_lang_contrib])
    except Exception as e:
        logger.error("Error during processing %d-%d-%d %d hr: %s" % (year, month, day, hour, e))


@w.task
@concurrency(1)
def country_rank():
    '''Activities per country and month.'''
    locs = {}
    for location in mongodb().locations.find({'country': {'$ne': None}}, {'_id': 1, 'country': 1}):
        locs[location['_id']] = location['country']['long_name']
    countries = {}
    default = lambda: {'year': defaultdict(int), 'month': defaultdict(lambda: defaultdict(int)), 'total': 0}
    for user in mongodb().users_stats.find({'info.location': {'$ne': None}},
                                           {'month': 1, 'info.location': 1, 'contrib': 1}):
        country = locs.get(user['info']['location'].lower(), None)
        if country:
            if country not in countries:
                countries[country] = default()
                countries[country]['users'] = 0
                countries[country]['contrib'] = defaultdict(default)
                countries[country]['display'] = {'en': country}
            countries[country]['users'] += 1
            for year in user.get('month', {}):
                for month in user['month'][year]:
                    countries[country]['month'][year][month] += user['month'][year][month]
                    countries[country]['year'][year] += user['month'][year][month]
                    countries[country]['total'] += user['month'][year][month]
            cont = countries[country]['contrib']
            for lang in user.get('contrib', {}):
                for year in user['contrib'][lang]:
                    for month in user['contrib'][lang][year]:
                        cont[lang]['month'][year][month] += user['contrib'][lang][year][month]
                        cont[lang]['year'][year] += user['contrib'][lang][year][month]
                        cont[lang]['total'] += user['contrib'][lang][year][month]
    results = {country: translate.delay(country, from_lang='en', to_lang='zh') for country in countries}
    for country, result in results.items():
        try:
            countries[country]['display']['zh'] = result.get()
        except:
            logger.error("Error during translating %s." % country)
    stats = mongodb().country_stats
    for country in countries:
        stats.update({'_id': country}, {'$set': countries[country]}, True)


@w.task
@concurrency(1)
def city_rank():
    '''Activities per city and month.'''
    locs, countries = {}, {}
    for location in mongodb().locations.find({'locality.long_name': {'$ne': None}},
                                             {'_id': 1,
                                              'locality.long_name': 1,
                                              'country.long_name': 1,
                                              'administrative_area_level_1.long_name': 1}):
        locs[location['_id']] = (location.get('country', {}).get('long_name'),
                                 location.get('administrative_area_level_1', {}).get('long_name'),
                                 location['locality']['long_name'])
    localities = {}
    default = lambda: {'year': defaultdict(int), 'month': defaultdict(lambda: defaultdict(int)), 'total': 0}
    for user in mongodb().users_stats.find({'info.location': {'$ne': None}},
                                           {'month': 1, 'info.location': 1, 'contrib': 1}):
        country, state, city = locs.get(user['info']['location'].lower(), (None, None, None))
        if city:
            if city not in localities:
                localities[city] = default()
                localities[city]['users'] = 0
                localities[city]['contrib'] = defaultdict(default)
                localities[city]['country'] = country
                localities[city]['state'] = state
                localities[city]['display'] = {'en': city}
            localities[city]['users'] += 1
            for year in user.get('month', {}):
                for month in user['month'][year]:
                    localities[city]['month'][year][month] += user['month'][year][month]
                    localities[city]['year'][year] += user['month'][year][month]
                    localities[city]['total'] += user['month'][year][month]
            cont = localities[city]['contrib']
            for lang in user.get('contrib', {}):
                for year in user['contrib'][lang]:
                    for month in user['contrib'][lang][year]:
                        cont[lang]['month'][year][month] += user['contrib'][lang][year][month]
                        cont[lang]['year'][year] += user['contrib'][lang][year][month]
                        cont[lang]['total'] += user['contrib'][lang][year][month]
    results = {city: translate.delay(city, from_lang='en', to_lang='zh') for city in localities}
    for city, result in results.items():
        try:
            localities[city]['display']['zh'] = result.get()
        except:
            logger.error("Error during translating %s." % city)
    stats = mongodb().city_stats
    for city, value in localities.items():
        stats.update({'_id': city}, {'$set': value}, True)


@w.task
def translate(text, from_lang='en', to_lang='zh'):
    return Translator(to_lang=to_lang, from_lang=from_lang).translate(text.encode('utf8'))
