#!/usr/bin/env python
# -*- coding: utf-8 -*-

# monkey patch
from gevent import monkey
monkey.patch_all()

import zerorpc
from datetime import datetime, timedelta
from time import sleep


class RPCService(object):

    @zerorpc.stream
    def traverse(self):
        since = datetime(2012, 1, 1)
        while True:
            if since < datetime.today() - timedelta(days=3):
                yield [since.year, since.month, since.day, since.hour]
                since += timedelta(hours=1)
            else:
                sleep(3600*24)


s = zerorpc.Server(RPCService())
s.bind("tcp://0.0.0.0:4242")
s.run()
