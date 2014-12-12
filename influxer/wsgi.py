#!/usr/bin/env python
# -*- coding: utf-8 -*-

import base64
import re
try:
    from urllib.parse import parse_qs
except ImportError:
    from urlparse import parse_qs
import os

from influxdb.client import InfluxDBClient


gif = base64.b64decode('R0lGODlhAQABAIAAAP///////yH5BAEKAAEALAAAAAABAAEAAAICTAEAOw==')

regex = re.compile(r'^.*\.\d+$')

host = os.environ.get('INFLUXDB_HOST_IP', 'influx-master.local')
port = os.environ.get('INFLUXDB_PORT', 8086)
user = os.environ.get('INFLUXDB_USERNAME', 'root')
pwd = os.environ.get('INFLUXDB_PASSWORD', 'rootuserspassword')
db = os.environ.get('INFLUXDB_DB', 'influxdb')
client = InfluxDBClient(host, port, user, pwd, db)


def application(env, start_response):
    """

    :param env:
    :param start_response:
    :return:
    """
    if env['PATH_INFO'] == '/influx.gif':
        start_response('200 OK', [('Content-Typ', 'image/gif')])
        yield gif

        qs = parse_qs(env['QUERY_STRING'])

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
        except Exception as e:
            print(e)

    else:
        start_response('404 Not Found', [('Content-Type', 'text/plain')])
        yield 'Nothing Here'