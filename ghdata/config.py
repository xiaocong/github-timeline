#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from urlparse import urlparse

__all__ = ["MONGODB_URI", "REDIS_URI", "REDIS_HOST", "REDIS_PORT", "REDIS_DB", "GITHUB_CRENDENTIALS"]


MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
REDIS_URI = os.getenv("REDIS_URI", "redis://localhost:6379/1")
GITHUB_CRENDENTIALS = os.environ.get(
    "GITHUB_CRENDENTIALS",
    "02d0253edfa0f44fdfee:5f759bdc51b1a043ec90d2aaea0cedae1dea3bd2"
)

ru = urlparse(REDIS_URI)
REDIS_HOST = ru.hostname
REDIS_PORT = ru.port
if ru.path.find("/") == 0:
    REDIS_DB = int(ru.path[1:])
else:
    REDIS_DB = 0
