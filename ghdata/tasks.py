#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import

from .worker import worker as w, logger

import requests
import time
from datetime import datetime, timedelta
import functools
import math
from collections import defaultdict
from translate import Translator
from celery import group

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


@w.task(ignore_result=True)
def update_user(index, step):
    '''update user's info from github'''
    r = redis()
    users = r.zrevrange(_format("user"), index, index)
    if len(users) > 0:
        logger.info("Updating %s at %d." % (users[0], index))
        count = 0
        while not _update_user(users[0]) and count < 6:
            logger.info("Holding github user task for 10 minutes.")
            time.sleep(10 * 60)
            count += 1
        update_user.delay(index + step, step)
    else:
        r.srem(_format('update:users:index'), index % step)


def _update_user(username):
    username = username.lower()
    users = mongodb().users_stats
    user = users.find_one({"_id": username}, {"info": 1})

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
                         headers=headers, timeout=30)
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
            return False
    except:
        pass
    finally:
        if r:
            r.close()
    update_location.delay(location, username)
    return True


@w.task(ignore_result=True)
def update_location(location, username=None):
    '''get location data(contry, city...) from location name'''
    if location in [None, ""]:
        return

    logger.info("Retrieving location %s" % location)
    location = location.lower()
    locations = mongodb().locations

    loc = locations.find_one({"_id": location})
    if not loc:
        loc = {"_id": location}
        loc.update(geo_info(location) or {})
        locations.update({"_id": location}, loc, True)
    if username:
        username = username.lower()
        loc_info = {
            'country': loc.get('country', {}).get('long_name', None),
            'state': loc.get('administrative_area_level_1', {}).get('long_name', None),
            'city': loc.get('locality', {}).get('long_name', None),
            'timezone': loc.get('timezone', 0)
        }
        mongodb().users_stats.update({'_id': username}, {'$set': {'loc': loc_info}})


@w.task(ignore_result=True)
def update_all_users(step=30):
    '''Traverse all users and retrieve its info from github.'''
    r = redis()
    for i in range(30):
        if r.sadd(_format('update:users:index'), i):
            update_user.delay(i, step)


@w.task(ignore_result=True)
@concurrency(1)
def fetch_timeline(year=2012, month=3, day=1):
    '''worker process to go through all timeline data since 2012/3/1.'''
    since = datetime(year, month, day)
    hours = int((datetime.today() - since).total_seconds() / 3600)
    times = (since + timedelta(hours=i) for i in range(hours))
    group(fetch_worker.s(h.year, h.month, h.day, h.hour) for h in times)()


@w.task(time_limit=3600 * 4)
def fetch_worker(year, month, day, hour):
    '''fetch one hour's timeline data and save it to db.'''
    try:
        file_process(fetch_one(year, month, day, hour), [events_process, events_process_lang_contrib])
    except Exception as e:
        logger.error("Error during processing %d-%d-%d %d hr: %s" % (year, month, day, hour, e))


@w.task(time_limit=3600 * 8)
@concurrency(1)
def country_rank():
    '''Activities per country and month.'''
    default = lambda: {'year': defaultdict(int), 'month': defaultdict(lambda: defaultdict(int)), 'total': 0}
    countries = defaultdict(default)
    trans = {}
    for user in mongodb().users_stats.find({'loc.country': {'$ne': None}},
                                           {'month': 1, 'loc.country': 1, 'contrib': 1}):
        country = user['loc']['country']
        if not country:
            continue
        if country not in countries:
            countries[country]['users'] = 0
            countries[country]['contrib'] = defaultdict(default)
            countries[country]['display'] = {'en': country}
            trans[country] = translate.delay(country, to_lang='zh')
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
    stats = mongodb().country_stats
    for country, value in countries.items():
        try:
            value['display']['zh'] = trans[country].get() or country
        except:
            logger.error("Error during translating %s." % country)
        stats.update({'_id': country}, {'$set': value}, True)


@w.task(time_limit=3600 * 8)
@concurrency(1)
def city_rank():
    '''Activities per city and month.'''
    localities, trans = {}, {}
    default = lambda: {'year': defaultdict(int), 'month': defaultdict(lambda: defaultdict(int)), 'total': 0}
    for user in mongodb().users_stats.find({'loc.city': {'$ne': None}},
                                           {'month': 1, 'loc': 1, 'contrib': 1}):
        country, state, city = [user['loc'].get(t, None) for t in ['country', 'state', 'city']]
        if not city:
            continue
        if city not in localities:
            localities[city] = default()
            localities[city]['users'] = 0
            localities[city]['contrib'] = defaultdict(default)
            localities[city]['country'] = country
            localities[city]['state'] = state
            localities[city]['display'] = {'en': city}
            trans[city] = translate.delay(city, to_lang='zh')
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
    stats = mongodb().city_stats
    for city, value in localities.items():
        try:
            value['display']['zh'] = trans[city].get() or city
        except:
            logger.error("Error during translating %s." % city)
        stats.update({'_id': city}, {'$set': value}, True)


@w.task
def translate(text, to_lang='zh'):
    translation = mongodb().translation
    t = translation.find_one({'_id': text, to_lang: {'$ne': None}}, {to_lang: 1})
    result = t and t[to_lang]
    if not result:
        try:
            result = Translator(to_lang=to_lang, from_lang='en').translate(text.encode('utf8'))
            translation.update({'_id': text}, {'$set': {to_lang: result}}, True)
        except:
            result = None
    return result


@w.task
@concurrency(1)
def update_users_location():
    locs = {}
    for location in mongodb().locations.find({},
                                             {'locality.long_name': 1,
                                              'country.long_name': 1,
                                              'administrative_area_level_1.long_name': 1,
                                              'timezone': 1
                                              }):
        locs[location['_id']] = {
            'country': location.get('country', {}).get('long_name', None),
            'state': location.get('administrative_area_level_1', {}).get('long_name', None),
            'city': location.get('locality', {}).get('long_name', None),
            'timezone': location.get('timezone', 0)
        }
    users_stats = mongodb().users_stats
    for user in users_stats.find({'info.location': {'$ne': None}}):
        location = user['info']['location'].lower()
        if location in locs:
            users_stats.update({'_id': user['_id']}, {'$set': {'loc': locs[location]}})


@w.task
def rank():
    (country_rank.si() | city_rank.si())()

    now = datetime.now()
    year, month = (now.year - 1, 12) if now.month == 1 else (now.year, now.month - 1)
    key = 'month.%d.%2d' % (year, month)
    # get languages sorted by activity of last month in the world.
    langs = [lang['_id'] for lang in mongodb().languages.find().sort(key, -1).limit(25)]
    user_rank.delay(langs)


@w.task
def user_rank(langs, country='China', months=24):
    now = datetime.now()
    year, month = now.year - int(math.ceil((months - now.month + 1) / 12.)), (now.month - months - 1) % 12 + 1

    pipe = redis().pipeline()
    t_keys = {lang: _format('%s:%s' % (str(time.time()), lang)) for lang in langs}
    for i, user in enumerate(mongodb().users_stats.find({'loc.country': country,
                                                         'contrib': {'$ne': None},
                                                         'robot': {'$ne', True}},
                                                        {'contrib': 1, 'loc': 1})):
        for lang in user.get('contrib', {}):
            if lang in langs:
                c = user['contrib'][lang]
                v = sum(c[y][m] for y in c for m in c[y] if (int(y) > year or (int(y) == year and int(m) >= month)))
                pipe.zadd(t_keys[lang], user['_id'], v)
        i % 100 or pipe.execute()
    pipe.execute()
    for lang in langs:
        r_key = _format("country:{0}.lang:{1}:user".format(country, lang))
        try:
            pipe.delete(r_key).rename(t_keys[lang], r_key).execute()
        except:
            pass
