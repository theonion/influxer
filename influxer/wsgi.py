#!/usr/bin/env python
# -*- coding: utf-8 -*-

import base64
import logging
import logging.handlers
import re
try:
    from urllib.parse import parse_qs
except ImportError:
    from urlparse import parse_qs
import os

from influxdb.client import InfluxDBClient


logger = logging.getLogger('influxer')
logger.setLevel(logging.INFO)
log_file_name = os.environ.get('LOG_FILE_NAME', 'influxer.log')
handler = logging.handlers.RotatingFileHandler(log_file_name, maxBytes=5000000, backupCount=5)
logger.addHandler(handler)

gif = base64.b64decode('R0lGODlhAQABAIAAAP///////yH5BAEKAAEALAAAAAABAAEAAAICTAEAOw==')

regex = re.compile(r'^.*\.\d+$')

host = os.environ.get('INFLUXDB_HOST_IP', 'localhost')
port = os.environ.get('INFLUXDB_PORT', 8086)
user = os.environ.get('INFLUXDB_USERNAME', 'root')
pwd = os.environ.get('INFLUXDB_PASSWORD', 'root')
db = os.environ.get('INFLUXDB_DB', 'influxdb')
client = InfluxDBClient(host, port, user, pwd, db)
logger.info('connected to influxdb: {}:{}'.format(host, port))


def influxer_1(qs):
    """drop in replacement for tinytracker style event monitoring. this strips important information from the
    query string and pushes it to influxdb for storage.

    :param qs: the parsed query string
    """
    try:
        events = qs.get('event', [])
        for event in events:
            if re.match(regex, event):
                event_data = event.split('.')
                property = event_data[0]
                content_id = event_data[-1]
                body = [{
                    'name': property,
                    'columns': ['content_id', 'clicks'],
                    'points': [[content_id, 1]]
                }]
                res = client.write_points(body)
                logger.info('{} {}'.format(res, body))
    except Exception as e:
        logger.error(str(e))


def influxer_2(qs):
    """better replacement for tinytracker event monitoring. this uses the dotted tagging style available for
    influxdb v0.8.7+.

    :param qs: the parsed query string
    """
    try:
        events = qs.get('event', [])
        for event in events:
            if re.match(regex, event):
                event_data = event.split('.')
                content_id = event_data[-1]
                name = '.'.join(event_data[:-1])
                body = [{
                    'name': name,
                    'columns': ['content_id', 'clicks'],
                    'points': [[content_id, 1]],
                }]
                res = client.write_points(body)
                logger.info('{}, {}',format(res, body))
    except Exception as e:
        logger.error(str(e))


def application(env, start_response):
    if env['PATH_INFO'] == '/influx.gif':
        start_response('200 OK', [('Content-Typ', 'image/gif')])
        yield gif
        qs = parse_qs(env['QUERY_STRING'])
        influxer_1(qs)
    elif env['PATH_INFO'] == '/influxer2.gif':
        start_response('200 OK', [('Content-Typ', 'image/gif')])
        yield gif
        qs = parse_qs(env['QUERY_STRING'])
        influxer_2(qs)
    else:
        start_response('404 Not Found', [('Content-Type', 'text/plain')])
        yield 'Nothing Here'
