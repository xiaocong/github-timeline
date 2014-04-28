#!/usr/bin/env python
# -*- coding: utf-8 -*-

from gevent import monkey; monkey.patch_all()

from bottle import Bottle, request, response, run, abort
import bottle.ext.mongo
import bottle.ext.redis
import os
from datetime import datetime

from ghdata.config import MONGODB_URI, REDIS_HOST, REDIS_PORT, REDIS_DB
from ghdata.db import format_key as _format

app = Bottle()
app.install(bottle.ext.mongo.MongoPlugin(uri=MONGODB_URI, db="github", json_mongo=True))
app.install(bottle.ext.redis.RedisPlugin(host=REDIS_HOST, port=REDIS_PORT, database=REDIS_DB))

from beaker.cache import CacheManager
from beaker.util import parse_cache_config_options

cache_opts = {
    'cache.type': 'file',
    'cache.data_dir': '/tmp/ghdata/data',
    'cache.lock_dir': '/tmp/ghdata/lock'
}

cache = CacheManager(**parse_cache_config_options(cache_opts))


@app.hook("after_request")
def crossDomianHook():
    response.headers["Access-Control-Allow-Origin"] = "*"


@app.route(path="/languages", method="OPTIONS")
@app.route(path="/rank", method="OPTIONS")
def options1(method, *args):
    return options(*args)


@app.route(path="/users/:id", method="OPTIONS")
def options2(id, *args):
    return options(*args)


def options(*args):
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE"
    if request.headers.get("Access-Control-Request-Headers"):
        response.headers["Access-Control-Allow-Headers"] = request.headers["Access-Control-Request-Headers"]


@app.get("/users/:id")
def user(id, rdb, mongodb):
    user = mongodb.users_stats.find_one({'_id': id})
    if not user:
        return abort(404)
    user['rank'] = {}
    langs = [lang for lang in user.get('contrib', {})]
    pipe = rdb.pipeline()
    if user.get('loc', {}).get('country') == 'China':
        for lang in langs:
            key = _format("country:{0}.lang:{1}:user".format(user.get('loc', {}).get('country', 'China'), lang))
            pipe.zrevrank(key, id)
        user['rank']['China'] = {lang: rank + 1 for lang, rank in zip(langs, pipe.execute())}
    for lang in langs:
        key = _format("lang:{0}:user".format(lang))
        pipe.zrevrank(key, id)
    user['rank']['World'] = {lang: rank + 1 for lang, rank in zip(langs, pipe.execute())}
    return user


@app.get("/languages")
def languages(mongodb):
    now = datetime.now()
    year, month = (now.year-1, 12) if now.month == 1 else (now.year, now.month-1)
    key = 'month.%d.%2d' % (year, month)
    # get languages sorted by activity of last month in the world.
    return {'data': [lang['_id'] for lang in mongodb.languages.find().sort(key, -1).limit(20)]}


@app.get("/rank")
def rank(rdb, mongodb):
    country = request.query.country or 'China'
    lang = request.query.language or 'JavaScript'
    page = int(request.query.page or 0)
    page_count = int(request.query.page_count or 50)
    return _rank(lang, country, page, page_count, rdb, mongodb)


@cache.cache("rank", expire=3600*12)
def _rank(lang, country, page, page_count, rdb, mongodb):
    key = _format("country:{0}.lang:{1}:user".format(country, lang))
    total = rdb.zcard(key)
    pages = total/page_count + (total % page_count and 1)
    users = rdb.zrevrange(key, page*page_count, (page+1)*page_count - 1)
    w_key = _format("lang:{0}:user".format(lang))
    pipe = rdb.pipeline()
    for u in users:
        pipe.zrevrank(w_key, u)
    w_ranks = pipe.execute()
    data = [mongodb.users_stats.find_one({'_id': u}, {'info': 1, 'contrib': 1}) for u in users]
    for i, (user, wr) in enumerate(zip(data, w_ranks)):
        user['rank'] = {country: page_count*page + i + 1, 'world': wr + 1}
    return {
        'pages': pages,
        'page': page,
        'page_count': page_count,
        'language': lang,
        'country': country,
        'data': data
    }


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    run(app=app, server="gevent", host="0.0.0.0", port=port)
