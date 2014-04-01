#!/usr/bin/env python
# -*- coding: utf-8 -*-

# monkey patch
from gevent import monkey
monkey.patch_all()

import gevent
import gevent.queue

import requests
import os
import time
from db import mongodb, redis, format_key as _format
from geo import geo_info

ghapi_url = "https://api.github.com/users/{username}"
geoname_url = "http://api.geonames.org/search"
_index = 0


def get_github_credential():
    github_credentials = os.environ.get(
        "GITHUB_CRENDENTIALS",
        "02d0253edfa0f44fdfee:5f759bdc51b1a043ec90d2aaea0cedae1dea3bd2"
    )
    return github_credentials.split(":")


def update_user(username, update=False):
    username = username.lower()
    users = mongodb().users_stats
    if update:
        user = users.find_one({"_id": username}, {"info": 1})
    else:
        user = users.find_one({"_id": username, "info": None})
    successful = True
    if user:
        info = user.get('info', {})
        etag = info.get("etag", None)
        location = info.get("location", None)

        r = None
        try:
            # Work out the authentication headers.
            auth = {}
            client_id, client_secret = get_github_credential()
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
                print("*** Limitation reached.")
                successful = False
        finally:
            if r:
                r.close()
        update_location(location)
    return successful


def update_location(location):
    if location in [None, ""]:
        return

    location = location.lower()
    locations = mongodb().locations
    loc = locations.find_one({"_id": location})
    if loc is None:
        loc = {"_id": location}
    if "timezone" not in loc or "country" not in loc:
        if "timezone" not in loc:
            info = geo_info(location)
            if info is not None:
                loc.update(info)
        if "country" not in loc:
            r = None
            try:
                params = {
                    "name": location,
                    "maxRows": 1,
                    "type": "json",
                    "username": "xiaocong"
                }
                r = requests.get(geoname_url, params=params)
                if r.status_code == requests.codes.ok:
                    data = r.json()
                    if data["geonames"]:
                        country = data["geonames"][0].get("countryName", None)
                        code = data["geonames"][0].get("countryCode", None)
                        if country is not None:
                            loc["country"] = {
                                "long_name": country,
                                "short_name": code
                            }
            finally:
                if r:
                    r.close()
        locations.update({"_id": location}, loc, True)


def worker(q):
    for name in q:
        try:
            while not update_user(name, True):
                print("Holding thread for 10 minutes.")
                time.sleep(10 * 60)
        except:
            pass


def update_users():
    q = gevent.queue.Queue(50)
    workers = [gevent.spawn(worker, q) for i in range(50)]

    r = redis()
    total = int(r.zcard(_format("user")))
    print("Total: %d users." % total)
    index = 0
    count = 100
    percent = 0.0
    while True:
        names = r.zrevrange(_format("user"), index, index + count)
        for name in names:
            q.put(name)
        if len(names) < count:
            break
        index += count
        if int(index * 10000. / total) / 100. > percent:
            percent = int(index * 10000. / total) / 100.
            print("Finish %.2f %%." % (percent))
    for i in range(len(workers)):
        q.put(StopIteration)
    gevent.joinall(workers)

if __name__ == "__main__":
    update_users()
