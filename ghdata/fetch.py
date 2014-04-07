#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import os.path
import re
import json
import requests
import shutil
import gzip
from tempfile import NamedTemporaryFile
import StringIO
from datetime import date
from collections import defaultdict

from .db import redis, mongodb, pipe as _pipe, format_key as _format

# The URL template for the GitHub Archive.
archive_url = ("http://data.githubarchive.org/"
               "{year}-{month:02d}-{day:02d}-{hour}.json.gz")
local_url = "./data/{year}-{month:02d}-{day:02d}-{hour}.json.gz"
date_re = re.compile(r"([0-9]{4})-([0-9]{2})-([0-9]{2})-([0-9]+)\.json.gz")

# mkdir data directory
os.path.exists('./data') or os.mkdir('./data')


def fetch_one(year, month, day, hour):
    '''Fecch one archived timeline.'''
    local_fn = local_url.format(year=year, month=month, day=day, hour=hour)
    if os.path.exists(local_fn):
        print '%s exists.' % local_fn
        return local_fn
    else:
        url = archive_url.format(year=year, month=month, day=day, hour=hour)
        r = None
        try:
            r = requests.get(url)
            if r.status_code == 200:
                f = NamedTemporaryFile("wb", delete=False)
                f.write(r.content)
                f.flush()
                os.fsync(f.fileno())
                f.close()
                shutil.move(f.name, local_fn)
                print("Fetching %s successded." % url)
                return local_fn
            else:
                print("Fetching %s failed." % url)
        except:
            return None
        finally:
            if r is not None:
                r.close()
    return None


def _gen_json(buf):
    line = buf.readline()
    while line:
        try:
            yield json.loads(line)
        except Exception as e:
            print "Error during load json: %s" % e
        line = buf.readline()


def file_process(filename, fns):
    if not filename or not os.path.exists(filename):
        return
    fns = fns if type(fns) is list else [fns]
    year, month, day, hour = map(int, date_re.findall(filename)[0])
    r = redis()
    repl = lambda m: ''
    for fn in fns:
        print('Processing %s with %s' % (filename, fn.__name__))
        fn_key = _format('function:%s' % fn.__name__)
        fn_value = "{year}-{month:02d}-{day:02d}-{hour}".format(
            year=year, month=month, day=day, hour=hour
        )
        if not r.sismember(fn_key, fn_value):
            with gzip.GzipFile(filename) as f:
                content = f.read().decode("utf-8", errors="ignore")
                content = re.sub(u"[^\\}]([\\n\\r\u2028\u2029]+)[^\\{]", repl, content)
                content = '}\n{"'.join(content.split('}{"'))
                buf = StringIO.StringIO(content)
                fn(_gen_json(buf), year, month, day, hour)
            r.sadd(fn_key, fn_value)


def _mongo_default():
    return defaultdict(lambda: defaultdict(int))


def events_process(events, year, month, day, hour):
    '''main events process method.'''
    weekday = date(year=year, month=month, day=day).strftime("%w")
    year_month = "{year}-{month:02d}".format(year=year, month=month)
    pipe = _pipe()
    users = defaultdict(_mongo_default)
    repos = defaultdict(_mongo_default)
    languages = defaultdict(_mongo_default)
    for event in events:
        actor = event["actor"]
        attrs = event.get("actor_attributes", {})
        if actor is None or attrs.get("type") != "User":
            # This was probably an anonymous event (like a gist event)
            # or an organization event.
            continue

        # Normalize the user name.
        key = actor.lower()

        # Get the type of event.
        evttype = event["type"]
        nevents = 1

        # Can this be called a "contribution"?
        contribution = evttype in ["IssuesEvent", "PullRequestEvent",
                                   "PushEvent"]

        # Increment the global sum histograms.
        pipe.incr(_format("total"), nevents)
        pipe.hincrby(_format("day"), weekday, nevents)
        pipe.hincrby(_format("hour"), hour, nevents)
        pipe.hincrby(_format("month"), year_month, nevents)
        pipe.zincrby(_format("user"), key, nevents)
        pipe.zincrby(_format("event"), evttype, nevents)

        # Event histograms.
        pipe.hincrby(_format("event:{0}:day".format(evttype)),
                     weekday, nevents)
        pipe.hincrby(_format("event:{0}:hour".format(evttype)),
                     hour, nevents)
        pipe.hincrby(_format("evnet:{0}:month".format(evttype)),
                     year_month, nevents)

        # User schedule histograms.
        incs = [
            'total',
            'day.%s' % weekday,
            'hour.%02d' % hour,
            'month.%04d.%02d' % (year, month),
            'event.%s.day.%s' % (evttype, weekday),
            'event.%s.hour.%02d' % (evttype, hour),
            'event.%s.month.%04d.%02d' % (evttype, year, month)
        ]
        for inc in incs:
            users[key]['$inc'][inc] += nevents
        # Parse the name and owner of the affected repository.
        repo = event.get("repository", {})
        owner, name, org = (repo.get("owner"), repo.get("name"),
                            repo.get("organization"))
        if owner and name:
            repo_name = "{0}/{1}".format(owner, name)

            # Save the social graph.
            users[key]['repos'][repo_name] += nevents
            repos[repo_name]['$inc']['total'] += nevents
            repos[repo_name]['$inc']['events.%s' % evttype] += nevents
            repos[repo_name]['$inc']['users.%s' % key] += nevents

            # Do we know what the language of the repository is?
            language = repo.get("language")
            if language:
                # Which are the most popular languages?
                languages[language]['$inc']['total'] += nevents
                languages[language]['$inc']['events.%s' % evttype] += nevents
                languages[language]['$inc']['month.%d.%2d' % (year, month)] += nevents

                # The most used language of users
                users[key]['$inc']['lang.%s' % language] += nevents

                # Who are the most important users of a language?
                if contribution:
                    pipe.zincrby(_format("lang:{0}:user".format(language)),
                                 key, nevents)
    users_stats = mongodb().users_stats
    for key in users:
        users_stats.update({'_id': key}, {'$inc': users[key]['$inc']}, True)
        for repo_name in users[key]['repos']:
            users_stats.update(
                {'_id': key, 'repos.repo': {'$ne': repo_name}},
                {'$addToSet': {'repos': {'repo': repo_name, 'events': 0}}},
                False
            )
            users_stats.update(
                {'_id': key, 'repos.repo': repo_name},
                {'$inc': {'repos.$.events': users[key]['repos'][repo_name]}},
                False
            )
    del users
    languages_stats = mongodb().languages
    for key in languages:
        languages_stats.update({'_id': key},
                               {'$inc': languages[key]['$inc']},
                               True)
    del languages
    repos_stats = mongodb().repositories
    for key in repos:
        repos_stats.update({'_id': key},
                           {'$inc': repos[key]['$inc']},
                           True)
    del repos
    pipe.execute()


def events_process_lang_contrib(events, year, month, day, hour):
    '''lang contribution process method.'''
    users = defaultdict(_mongo_default)
    for event in events:
        actor = event["actor"]
        attrs = event.get("actor_attributes", {})
        if actor is None or attrs.get("type") != "User":
            # This was probably an anonymous event (like a gist event)
            # or an organization event.
            continue

        # Normalize the user name.
        key = actor.lower()

        # Get the type of event.
        evttype = event["type"]
        nevents = 1

        # Can this be called a "contribution"?
        contribution = evttype in ["IssuesEvent", "PullRequestEvent", "PushEvent"]

        repo = event.get("repository", {})
        owner, name, org, language = (repo.get("owner"),
                                      repo.get("name"),
                                      repo.get("organization"),
                                      repo.get("language"))
        if owner and name and language and contribution:
            # The most used language of users
            users[key]['$inc']['contrib.%s.%d.%02d' % (language, year, month)] += nevents

    users_stats = mongodb().users_stats
    for key in users:
        users_stats.update({'_id': key}, {'$inc': users[key]['$inc']}, True)
    del users
