#!/usr/bin/env python
# -*- coding: utf-8 -*-

# monkey patch
from gevent import monkey
monkey.patch_all()

import gevent
import gevent.queue
import os
import re
import json
import requests
import shutil
import gzip
from tempfile import NamedTemporaryFile
from datetime import datetime, timedelta
from time import sleep
import StringIO
from datetime import date

import db
from db import format_key as _format

# The URL template for the GitHub Archive.
archive_url = ("http://data.githubarchive.org/"
               "{year}-{month:02d}-{day:02d}-{hour}.json.gz")
local_url = "./data/{year}-{month:02d}-{day:02d}-{hour}.json.gz"
date_re = re.compile(r"([0-9]{4})-([0-9]{2})-([0-9]{2})-([0-9]+)\.json.gz")


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


def file_process(filename, fn):
    print('Processing %s with %s' % (filename, fn.__name__))
    if not filename or not os.path.exists(filename):
        return
    year, month, day, hour = map(int, date_re.findall(filename)[0])
    r = db.redis()
    fn_key = _format('function:%s' % fn.__name__)
    fn_value = "{year}-{month:02d}-{day:02d}-{hour}".format(
        year=year, month=month, day=day, hour=hour
    )
    if r.sismember(fn_key, fn_value):
        return

    with gzip.GzipFile(filename) as f:
        repl = lambda m: ''
        content = f.read().decode("utf-8", errors="ignore")
        content = re.sub(u"[^\\}]([\\n\\r\u2028\u2029]+)[^\\{]", repl, content)
        content = '}\n{"'.join(content.split('}{"'))
        buf = StringIO.StringIO(content)
        fn(_gen_json(buf), year, month, day, hour)
    r.sadd(fn_key, fn_value)


def events_process(events, year, month, day, hour):
    weekday = date(year=year, month=month, day=day).strftime("%w")
    year_month = "{year}-{month:02d}".format(year=year, month=month)
    pipe = db.pipe()
    users_stats = db.mongodb().users_stats
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
        user_update = {
            '$inc': {
                'day.%s' % weekday: nevents,
                'hour.%02d' % hour: nevents,
                'month.%04d.%02d' % (year, month): nevents,
                'event.%s.day.%s' % (evttype, weekday): nevents,
                'event.%s.hour.%02d' % (evttype, hour): nevents,
                'event.%s.month.%04d.%02d' % (evttype, year, month): nevents
            }
        }

        # Parse the name and owner of the affected repository.
        repo = event.get("repository", {})
        owner, name, org = (repo.get("owner"), repo.get("name"),
                            repo.get("organization"))
        if owner and name:
            repo_name = "{0}/{1}".format(owner, name)
            pipe.zincrby(_format("repo"), repo_name, nevents)

            # Save the social graph.
            pipe.zincrby(_format("social:user:{0}".format(key)),
                         repo_name, nevents)
            pipe.zincrby(_format("social:repo:{0}".format(repo_name)),
                         key, nevents)

            # Do we know what the language of the repository is?
            language = repo.get("language")
            if language:
                # Which are the most popular languages?
                pipe.zincrby(_format("lang"), language, nevents)

                # Total number of pushes.
                if evttype == "PushEvent":
                    pipe.zincrby(_format("pushes:lang"), language, nevents)

                user_update['$inc']['lang.%s' % language] = nevents

                # Who are the most important users of a language?
                if contribution:
                    pipe.zincrby(_format("lang:{0}:user".format(language)),
                                 key, nevents)
        users_stats.update({'_id': key}, user_update, True)
    pipe.execute()


def traverse_all(fn):
# def fetch_all(since=datetime(2011, 2, 12)):
    q = gevent.queue.Queue(32)

    def worker():
        for year, month, day, hour in q:
            filename = fetch_one(year, month, day, hour)
            if filename:
                try:
                    file_process(filename, fn)
                except Exception as e:
                    print "Error during processing %s: %s" % (filename, e)

    workers = [gevent.spawn(worker) for i in range(8)]
    since = datetime(2012, 1, 1)
    while True:
        if since < datetime.today() - timedelta(days=3):
            q.put([since.year, since.month, since.day, since.hour])
            since += timedelta(hours=1)
        else:
            sleep(3600 * 24)

    for w in workers:
        q.put(StopIteration)
    gevent.joinall(workers)

if __name__ == "__main__":
    traverse_all(events_process)
