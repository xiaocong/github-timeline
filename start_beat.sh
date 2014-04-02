#!/bin/sh

celery beat --app=ghdata.worker:worker --detach -f /tmp/celeryd.beat.log --pidfile=/tmp/celeryd.beat.pid
