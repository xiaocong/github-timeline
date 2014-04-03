#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import (division, print_function, absolute_import,
                        unicode_literals)

__all__ = ["geo_info"]

import re
import logging
import requests
from time import sleep

tz_re = re.compile(r"<offset>([\-0-9\.]+)</offset>")
goapi_url = "http://maps.googleapis.com/maps/api/geocode/json"
mqapi_url = "http://open.mapquestapi.com/geocoding/v1/address"
tzapi_url = "http://www.earthtools.org/timezone-1.1/{lat}/{lng}"


def _google_geocode(location):
    while True:
        # Submit the request.
        params = {"address": location, "sensor": "false"}
        r = None
        try:
            r = requests.get(goapi_url, params=params)
            if r.status_code != requests.codes.ok:
                return None
        finally:
            if r:
                r.close()

        data = r.json()

        # Try not to go over usage limits.
        if data.get("status", None) == "OVER_QUERY_LIMIT":
            logging.info("Over geo query limit, sleep 1 hour...")
            sleep(60 * 60)
            continue
        # Parse the results.
        results = data.get("results", [])
        if not len(results):
            return None

        # update geo info
        types = ["sublocality", "locality", "administrative_area_level_1", "country"]
        info = {"loc": results[0].get("geometry", {}).get("location", None)}
        for addr in results[0].get("address_components", []):
            info.update({type: addr for type in types if type in addr.get("types", [])})
        return info


def geocode(location):
    return _google_geocode(location)


def geo_info(location):
    # Start by geocoding the location string.
    geo = geocode(location)
    if geo is None:
        logging.warn("Couldn't resolve location for {0}".format(location))
        return None

    r = None
    try:
        # Resolve the timezone associated with these coordinates.
        r = requests.get(tzapi_url.format(**geo["loc"]))
        if r.status_code != requests.codes.ok:
            logging.warn("Timezone zone request failed:\n{0}".format(r.url))
            return geo

        # Parse the results to try to work out the time zone.
        matches = tz_re.findall(r.text)
        if len(matches):
            geo["timezone"] = int(float(matches[0]))
            return geo
    finally:
        if r:
            r.close()

    logging.warn("Timezone result formatting is broken.\n{0}".format(r.url))
    return geo
