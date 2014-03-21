#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import (division, print_function, absolute_import,
                        unicode_literals)

__all__ = ["estimate_timezone"]

import re
import logging
import requests

from db import pipe as _pipe, format_key

tz_re = re.compile(r"<offset>([\-0-9\.]+)</offset>")
goapi_url = "http://maps.googleapis.com/maps/api/geocode/json"
mqapi_url = "http://open.mapquestapi.com/geocoding/v1/address"
tzapi_url = "http://www.earthtools.org/timezone-1.1/{lat}/{lng}"


def _google_geocode(location):
    # Check for quota limits.
    pipe = _pipe()
    usage_key = format_key("google_usage_limit")
    usage = pipe.get(usage_key).execute()[0]
    if usage is not None:
        logging.warn("Skipping Google geocode request for usage limits.")
        return None

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
    status = data.get("status", None)
    if status == "OVER_QUERY_LIMIT":
        pipe.set(usage_key, 1).expire(usage_key, 60 * 60)
        pipe.execute()
        return None

    # Parse the results.
    results = data.get("results", [])
    if not len(results):
        return None

    # Find the coordinates.
    info = {"loc": results[0].get("geometry", {}).get("location", None)}
    for addr in results[0].get("address_components", []):
        for type in ["sublocality",
                     "locality",
                     "administrative_area_level_1",
                     "country"]:
            if type in addr.get("types", []):
                info[type] = addr
    return info


def geocode(location):
    # Try Google first.
    try:
        loc = _google_geocode(location)
    except Exception as e:
        logging.warn("Google geocoding failed with:\n{0}".format(e))
        loc = None

    return loc


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
