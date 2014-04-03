#!/bin/sh

celery worker --app=ghdata.worker:worker --detach -P gevent -c 10 -Q celery --loglevel=INFO --logfile=/tmp/celeryd.assigner.log --pidfile=/tmp/celeryd.assigner.pid
celery beat --app=ghdata.worker:worker --detach -f /tmp/celeryd.beat.log --pidfile=/tmp/celeryd.beat.pid
