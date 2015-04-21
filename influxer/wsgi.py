#!/usr/bin/env python
# -*- coding: utf-8 -*-

import base64
from collections import Counter
import logging

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
handler = logging.handlers.RotatingFileHandler(log_file_name, maxBytes=5000000, backupCount=5)
logger.addHandler(handler)


def send_data(events, additional):
    """creates data point payloads and sends them to influxdb
    """
    bodies = {}
    for (site, content_id), count in events.items():
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
            client.write_points([{
                "name": site,
                "columns": ["content_id", "event", "path", "value"],
                "points": points,
            }])
        except Exception as e:
            logger.error(str(e))


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
                logger.error(str(e))
                break

        # after tabulating, spawn a new thread to send the data to influxdb
        if len(events):
            gevent.spawn(send_data, events, additional)


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
            site = params.get("site")
            content_id = params.get("content_id")
            event = params.get("event")
            path = params.get("path")
            events_queue.put((site, content_id, event, path))
        except Exception as e:
            logger.error(str(e))

    else:
        start_response("404 Not Found", [("Content-Type", "text/plain")])
        yield "Nothing Here"
