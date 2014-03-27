#!/usr/bin/env python
# -*- coding: utf-8 -*-

# monkey patch
from gevent import monkey
monkey.patch_all()

import gevent
import gevent.queue

from db import mongodb


total = 0


def trans(info):
    global total
    users_stats = mongodb().users_stats
    users_stats.update({'_id': info['_id']}, {'$set': {'info': info}}, True)
    total -= 1
    print 'Left %d' % total


def worker(q):
    for info in q:
        trans(info)
    print 'Done'


def trans_all():
    global total
    q = gevent.queue.Queue(32)
    workers = [gevent.spawn(worker, q) for i in xrange(16)]

    users_info = mongodb().users_info
    total = users_info.find().count()
    print 'Total %d' % total
    for info in users_info.find():
        q.put(info)
    for w in workers:
        q.put(StopIteration)
    gevent.joinall(workers)
    print 'All done'


if __name__ == '__main__':
    trans_all()
