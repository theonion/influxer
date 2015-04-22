#!/usr/bin/env python
# -*- coding: utf-8 -*-

import base64
from collections import Counter
import logging
from logging import handlers
import re

try:
    from urllib.parse import parse_qs
except ImportError:
    from urlparse import parse_qs
import os

import gevent
from gevent import queue
from influxdb.client import InfluxDBClient


# set up influxdb client
host = os.environ.get("INFLUXER_INFLUXDB_HOST_IP", "localhost")
port = os.environ.get("INFLUXER_INFLUXDB_PORT", 8086)
user = os.environ.get("INFLUXER_INFLUXDB_USERNAME", "root")
pwd = os.environ.get("INFLUXER_INFLUXDB_PASSWORD", "root")
db = os.environ.get("INFLUXER_INFLUXDB_DB", "influxdb")
client = InfluxDBClient(host, port, user, pwd, db)

# build the response gif
gif = base64.b64decode("R0lGODlhAQABAIAAAP///////yH5BAEKAAEALAAAAAABAAEAAAICTAEAOw==")

# init the gevent queue
events_queue = queue.Queue()
flush_interval = os.environ.get("INFLUXER_FLUSH_INTERVAL", 60)  # seconds

# init the logger
logger = logging.getLogger('influxer')
logger.setLevel(logging.INFO)
log_file_name = os.environ.get('INFLUXER_LOG_FILE_NAME', 'influxer.log')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler = handlers.RotatingFileHandler(log_file_name, maxBytes=5000000, backupCount=5)
handler.setFormatter(formatter)
logger.addHandler(handler)

# init content id regex
content_id_regex = re.compile(r"\d+")


def send_point_data(events, additional):
    """creates data point payloads and sends them to influxdb
    """
    logger.info('send_point_data')
    bodies = {}
    for (site, content_id), count in events.items():
        if not len(site) or not len(content_id):
            continue

        # influxdb will take an array of arrays of values, cutting down on the number of requests
        # needed to be sent to it wo write data
        bodies.setdefault(site, [])

        # get additional info
        event, path = additional.get((site, content_id), (None, None))

        # append the point
        bodies[site].append([content_id, event, path, count])

    for site, points in bodies.items():
        # send payload to influxdb
        try:
            data = [{
                "name": site,
                "columns": ["content_id", "event", "path", "value"],
                "points": points,
            }]
            logger.debug(str(data))
            client.write_points(data)
        except Exception as e:
            logger.exception(e)


def send_trending_data(events):
    """creates data point payloads for trending data to influxdb
    """
    logger.info('send_trending_data')
    bodies = {}

    # sort the values
    top_hits = sorted(
        [(key, count) for key, count in events.items()],
        key=lambda x: x[1],
        reverse=True
    )[:100]

    # build up points to be written
    for (site, content_id), count in top_hits:
        if not len(site) or not re.match(content_id_regex, content_id):
            continue

        # add point
        bodies.setdefault(site, [])
        bodies[site].append([content_id, count])

    for site, points in bodies.items():
        # create name
        name = "{}-trending".format(site)
        # send payload to influxdb
        try:
            data = [{
                "name": name,
                "columns": ["content_id", "value"],
                "points": points,
            }]
            logger.debug(str(data))
            client.write_points(data)
        except Exception as e:
            logger.exception(e)


def count_events():
    """pulls data from the queue, tabulates it and spawns a send event
    """
    while 1:
        # sleep and let the queue build up
        gevent.sleep(flush_interval)

        # init the data points containers
        events = Counter()
        additional = {}

        # flush the queue
        while 1:
            try:
                site, content_id, event, path = events_queue.get_nowait()
                events[(site, content_id)] += 1
                if event and path:
                    additional[(site, content_id)] = (event, path)
            except queue.Empty:
                break
            except Exception as e:
                logger.exception(e)
                break

        # after tabulating, spawn a new thread to send the data to influxdb
        if len(events):
            gevent.spawn(send_point_data, events, additional)
            gevent.spawn(send_trending_data, events)


# create the wait loop
gevent.spawn(count_events)


def application(env, start_response):
    """wsgi application
    """
    path = env["PATH_INFO"]

    if path == "/influx.gif":
        # send response
        start_response("200 OK", [("Content-Type", "image/gif")])
        yield gif

        # parse the query params and stick them in the queue
        params = parse_qs(env["QUERY_STRING"])
        try:
            site = params.get("site", [""])[0]
            content_id = params.get("content_id", [""])[0]
            event = params.get("event", [""])[0]
            path = params.get("path", [""])[0]
            events_queue.put((site, content_id, event, path))
            logger.debug(str((site, content_id, event, path)))
        except Exception as e:
            logger.exception(e)

    else:
        start_response("404 Not Found", [("Content-Type", "text/plain")])
        yield "Nothing Here"
