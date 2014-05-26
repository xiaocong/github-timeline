#!/bin/sh

celery multi start 4 --app=ghdata.worker:worker -Q:1 github -Q:2 geo -Q:3 fetch -Q:4 stats -c:1 15 -c:2 10 -c:3 2 -c:4 40 --loglevel=INFO --logfile=/tmp/celeryd.${USER}%n.log --pidfile=/tmp/celeryd.${USER}%n.pid
