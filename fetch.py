#!/usr/bin/env python
# -*- coding: utf-8 -*-

# monkey patch
from gevent import monkey
monkey.patch_all()

import gevent
import gevent.queue
from datetime import datetime, timedelta, date
import os
import re
import json
import requests
import shutil
import gzip
from tempfile import NamedTemporaryFile

from db import redis
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


def fetch_and_process(q, year, month, day, hour, fn):
    q.put((fetch_one(year, month, day, hour), fn))


def data_processer(q):
    for filename, fn in q:
        if type(fn) is not list:
            fn = [fn]
        for func in fn:
            try:
                _process(filename, func)
            except Exception as e:
                print("Error during processing %s." % filename)
                print(e)


def _gen_json(lines):
    for line in lines:
        try:
            yield json.loads(line)
        except Exception as e:
            print "Error during load json: %s" % e


def _process(filename, fn):
    print('Processing %s with %s' % (filename, fn.__name__))
    if not filename or not os.path.exists(filename):
        return
    year, month, day, hour = map(int, date_re.findall(filename)[0])
    r = redis()
    fn_key = _format('function:%s' % fn.__name__)
    fn_value = "{year}-{month:02d}-{day:02d}-{hour}".format(
        year=year, month=month, day=day, hour=hour
    )
    if r.sismember(fn_key, fn_value):
        return

    pipe = r.pipeline()
    fn_index_key = _format('function:%s:index' % fn.__name__)
    fn_index_values = []

    def _process_lines(index, lines):
        fn_index_value = "{year}-{month:02d}-{day:02d}-{hour}-{index}".format(
            year=year, month=month, day=day, hour=hour, index=index
        )
        pipe.sismember(fn_index_key, fn_index_value)
        if pipe.execute()[0]:
            return

        data = _gen_json(lines)
        fn(pipe, data, year, month, day, hour)
        pipe.sadd(fn_index_key, fn_index_value)
        pipe.execute()
        fn_index_values.append(fn_index_value)

    buf_lines = 1000
    with gzip.GzipFile(filename) as f:
        repl = lambda m: ''
        content = f.read().decode("utf-8", errors="ignore")
        content = re.sub(u"[^\\}]([\\n\\r\u2028\u2029]+)[^\\{]", repl, content)
        content = '}\n{"'.join(content.split('}{"'))
        lines = content.splitlines()
        index = 0
        while index < len(lines):
            print('Processing %s: lines %d' % (filename, index))
            try:
                _process_lines(index/buf_lines, lines[index:index+buf_lines])
            except Exception as e:
                print str(e)
            index += buf_lines
    for k in fn_index_values:
        pipe.srem(fn_index_key, k)
    pipe.sadd(fn_key, fn_value)
    pipe.execute()


def process_user(pipe, data, year, month, day, hour):
    weekday = date(year=year, month=month, day=day).strftime("%w")
    year_month = "{year}-{month:02d}".format(year=year, month=month)
    for event in data:
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
        pipe.hincrby(_format("user:{0}:day".format(key)), weekday, nevents)
        pipe.hincrby(_format("user:{0}:hour".format(key)), hour, nevents)
        pipe.hincrby(_format("user:{0}:month".format(key)), year_month, nevents)

        # User event type histogram.
        pipe.zincrby(_format("user:{0}:event".format(key)),
                     evttype, nevents)
        pipe.hincrby(_format("user:{0}:event:{1}:day".format(key,
                                                             evttype)),
                     weekday, nevents)
        pipe.hincrby(_format("user:{0}:event:{1}:hour".format(key,
                                                              evttype)),
                     hour, nevents)
        pipe.hincrby(_format("user:{0}:event:{1}:month".format(key,
                                                               evttype)),
                     year_month, nevents)

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

                pipe.zincrby(_format("user:{0}:lang".format(key)),
                             language, nevents)

                # Who are the most important users of a language?
                if contribution:
                    pipe.zincrby(_format("lang:{0}:user".format(language)),
                                 key, nevents)


def fetch_all(since=datetime(2011, 2, 12)):
    today = datetime.today()
    queue = gevent.queue.Queue()
    worker = gevent.spawn(data_processer, queue)
    while since < today:
        procs = [gevent.spawn(fetch_and_process,
                              queue,
                              since.year,
                              since.month,
                              since.day,
                              since.hour + i,
                              process_user) for i in range(24)]
        gevent.joinall(procs)
        since += timedelta(days=1)
    queue.put(StopIteration)
    worker.join()

if __name__ == "__main__":
    fetch_all()
