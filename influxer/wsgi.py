#!/usr/bin/env python
# -*- coding: utf-8 -*-

import base64
from collections import Counter
from datetime import datetime, timedelta
import logging
from logging import handlers
import json
import re
import os

import gevent
from gevent import queue
from influxdb.client import InfluxDBClient
import pylibmc

try:
    from urllib.parse import parse_qs
except ImportError:
    from urlparse import parse_qs


# build the response gif
GIF = base64.b64decode("R0lGODlhAQABAIAAAP///////yH5BAEKAAEALAAAAAABAAEAAAICTAEAOw==")


# init influxdb client
host = os.environ.get("INFLUXER_INFLUXDB_HOST_IP", "localhost")
port = os.environ.get("INFLUXER_INFLUXDB_PORT", 8086)
user = os.environ.get("INFLUXER_INFLUXDB_USERNAME", "root")
pwd = os.environ.get("INFLUXER_INFLUXDB_PASSWORD", "root")
db = os.environ.get("INFLUXER_INFLUXDB_DB", "influxdb")
INFLUXDB_CLIENT = InfluxDBClient(host, port, user, pwd, db)


# init the logger
LOGGER = logging.getLogger("influxer")
LOGGER.setLevel(logging.INFO)
log_file_name = os.environ.get("INFLUXER_LOG_FILE_NAME", "influxer.log")
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler = handlers.RotatingFileHandler(log_file_name, maxBytes=5000000, backupCount=5)
handler.setFormatter(formatter)
LOGGER.addHandler(handler)


# init memcached client
memcached_hosts = os.environ.get("INFLUXER_MEMCACHED_HOSTS", "localhost").split(",")
memcached_prefix = "INFLUXER"
MEMCACHED_CLIENT = pylibmc.Client(memcached_hosts)


# init the gevent queue
EVENTS_QUEUE = queue.Queue()
FLUSH_INTERVAL = os.environ.get("INFLUXER_FLUSH_INTERVAL", 60)  # seconds


# init regexes
CONTENT_ID_REGEX = re.compile(r"\d+")
SERIES_REGEX = re.compile(r"^/\^.*\$/$")


# init default values for reading
DEFAULT_SERIES = "/^[a-z]+$/"
DEFAULT_GROUP_BY = "10m"
DEFAULT_TRENDING_LIMIT = 20


# init the cache time
MEMCACHED_EXPIRATION = 60 * 5  # 5 minutes


# writing stuff
def send_point_data(events, additional):
    """creates data point payloads and sends them to influxdb
    """
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
            INFLUXDB_CLIENT.write_points(data)
        except Exception as e:
            LOGGER.exception(e)


def send_trending_data(events):
    """creates data point payloads for trending data to influxdb
    """
    bodies = {}

    # sort the values
    top_hits = sorted(
        [(key, count) for key, count in events.items()],
        key=lambda x: x[1],
        reverse=True
    )[:100]

    # build up points to be written
    for (site, content_id), count in top_hits:
        if not len(site) or not re.match(CONTENT_ID_REGEX, content_id):
            continue

        # add point
        bodies.setdefault(site, [])
        bodies[site].append([content_id, count])

    for site, points in bodies.items():
        # create name
        name = "{}_trending".format(site)
        # send payload to influxdb
        try:
            data = [{
                "name": name,
                "columns": ["content_id", "value"],
                "points": points,
            }]
            INFLUXDB_CLIENT.write_points(data)
        except Exception as e:
            LOGGER.exception(e)


def count_events():
    """pulls data from the queue, tabulates it and spawns a send event
    """
    # wait loop
    while 1:
        # sleep and let the queue build up
        gevent.sleep(FLUSH_INTERVAL)

        # init the data points containers
        events = Counter()
        additional = {}

        # flush the queue
        while 1:
            try:
                site, content_id, event, path = EVENTS_QUEUE.get_nowait()
                events[(site, content_id)] += 1
                if event and path:
                    additional[(site, content_id)] = (event, path)
            except queue.Empty:
                break
            except Exception as e:
                LOGGER.exception(e)
                break

        # after tabulating, spawn a new thread to send the data to influxdb
        if len(events):
            gevent.spawn(send_point_data, events, additional)
            gevent.spawn(send_trending_data, events)


# reading stuff
def make_default_times():
    # make datetime objects
    now = datetime.now()
    today = datetime(now.year, now.month, now.day, 0, 0, 0)
    yesterday = today - timedelta(days=1, seconds=1)
    # stringify them for influxdb
    default_from = yesterday
    default_to = today
    # return datetimes
    return default_from, default_to, yesterday, today


def parse_datetime(value, str_format="%Y-%m-%dT%H:%M:%S"):
    try:
        return datetime.strptime(value, str_format)
    except ValueError:
        return None


def format_datetime(dt, str_format="%Y-%m-%dT%H:%M:%S"):
    return dt.strftime(str_format)


def update_series(series):
    # if the series is a regex, slice away the start and end values and drop in the pattern with escape patterns
    if re.match(SERIES_REGEX, series):
        series_name = series[2:-2]
        series_name += "\.all\.5m"
        series = "/^{name}$/".format(name=series_name)
    # otherwise tack it onto the end in plaintext
    else:
        series += ".all.5m"
    return series


def update_trending_series(series):
    # if the series is a regex, slice away the start and end values and drop in the pattern with escape patterns
    if re.match(SERIES_REGEX, series):
        series_name = series[2:-2]
        series_name += "\-trending\.content_id\.5m"
        series = "/^{name}$/".format(name=series_name)
    # otherwise tack it onto the end in plaintext
    else:
        series += '-trending.content_id.5m'
        series = '"{}"'.format(series)
    return series


def flatten_response(influx_res):
    response = {}
    for pset in influx_res:
        site = pset["name"]
        cols = pset["columns"]
        points = []
        for point in pset["points"]:
            points.append(dict(zip(cols, point)))
        response[site] = points
    return response


def pageviews(params):
    """takes a couple (optional) query parameters and queries influxdb and sends a modified response
    """
    # set up default values
    default_from, default_to, yesterday, _ = make_default_times()

    # get params
    try:
        series = params.get("site", [DEFAULT_SERIES])[0]
        from_date = params.get("from", [default_from])[0]
        to_date = params.get("to", [default_to])[0]
        group_by = params.get("group_by", [DEFAULT_GROUP_BY])[0]
    except Exception as e:
        LOGGER.exception(e)
        return json.dumps({"error": e.message}), "500 Internal Error"

    # check the cache
    cache_key = "{}:{}:{}:{}:{}:{}".format(memcached_prefix, "pageviews.json", series, from_date, to_date, group_by)
    try:
        data = MEMCACHED_CLIENT.get(cache_key)
        if data:
            return data, "200 OK"
    except Exception as e:
        LOGGER.exception(e)

    # parse from date
    from_date = parse_datetime(from_date)
    if from_date is None:
        return json.dumps({"error": "could not parse 'from'"}), "400 Bad Request"

    # parse to date
    to_date = parse_datetime(to_date)
    if to_date is None:
        return json.dumps({"error": "could not parse 'to'"}), "400 Bad Request"

    # influx will only keep non-aggregated data for a day, so if the from param is beyond that point
    # we need to update the series name to use the rolled up values
    rollup_query = False
    if from_date < yesterday:
        series = update_series(series)
        rollup_query = True

    # format times
    from_date = format_datetime(from_date)
    to_date = format_datetime(to_date)

    # build out the query
    if not rollup_query:
        query = "SELECT sum(value) as value " \
                "FROM {series} " \
                "WHERE time > '{from_date}' " \
                "AND time < '{to_date}' " \
                "AND event =~ /^pageview$/ " \
                "GROUP BY time({group_by}) " \
                "fill(0);"
    else:
        query = "SELECT sum(value) as value " \
                "FROM {series} " \
                "WHERE time > '{from_date}' " \
                "AND time < '{to_date}' " \
                "GROUP BY time({group_by}) " \
                "fill(0);"
    args = {"series": series, "from_date": from_date, "to_date": to_date, "group_by": group_by}

    # send the request
    try:
        res = INFLUXDB_CLIENT.query(query.format(**args))

    # capture errors and send them back along with the query (for inspection/debugging)
    except Exception as e:
        LOGGER.exception(e)
        return json.dumps({"error": e.message, "query": query.format(**args)}), "500 Internal Error"

    # build the response object
    response = flatten_response(res)
    res = json.dumps(response)

    # cache the response
    try:
        MEMCACHED_CLIENT.set(cache_key, res, time=MEMCACHED_EXPIRATION)
    except Exception as e:
        LOGGER.exception(e)

    return res, "200 OK"


def embedviews(params):
    """takes a couple (optional) query parameters and queries influxdb and sends a modified response
    """
    # set up default values
    default_from, default_to, yesterday, _ = make_default_times()

    # get params
    try:
        series = params.get("site", [DEFAULT_SERIES])[0]
        from_date = params.get("from", [default_from])[0]
        to_date = params.get("to", [default_to])[0]
        group_by = params.get("group_by", [DEFAULT_GROUP_BY])[0]
    except Exception as e:
        LOGGER.exception(e)
        return json.dumps({"error": e.message}), "500 Internal Error"

    # check the cache
    cache_key = "{}:{}:{}:{}:{}:{}".format(memcached_prefix, "embedviews.json", series, from_date, to_date, group_by)
    try:
        data = MEMCACHED_CLIENT.get(cache_key)
        if data:
            return data, "200 OK"
    except Exception as e:
        LOGGER.exception(e)

    # parse from date
    from_date = parse_datetime(from_date)
    if from_date is None:
        return json.dumps({"error": "could not parse 'from'"}), "400 Bad Request"

    # parse to date
    to_date = parse_datetime(to_date)
    if to_date is None:
        return json.dumps({"error": "could not parse 'to'"}), "400 Bad Request"

    # influx will only keep non-aggregated data for a day, so if the from param is beyond that point
    # we need to update the series name to use the rolled up values
    if from_date < yesterday:
        series = update_series(series)

    # format times
    from_date = format_datetime(from_date)
    to_date = format_datetime(to_date)

    # build out the query
    query = "SELECT sum(value) as value " \
            "FROM {series} " \
            "WHERE time > '{from_date}' " \
            "AND time < '{to_date}' " \
            "AND event =~ /^embedview$/ " \
            "GROUP BY time({group_by}) " \
            "fill(0);"
    args = {"series": series, "from_date": from_date, "to_date": to_date, "group_by": group_by}

    # send the request
    try:
        res = INFLUXDB_CLIENT.query(query.format(**args))

    # capture errors and send them back along with the query (for inspection/debugging)
    except Exception as e:
        LOGGER.exception(e)
        return json.dumps({"error": e.message, "query": query.format(**args)}), "500 Internal Error"

    # build the response object
    response = flatten_response(res)
    res = json.dumps(response)

    # cache the response
    try:
        MEMCACHED_CLIENT.set(cache_key, res, time=MEMCACHED_EXPIRATION)
    except Exception as e:
        LOGGER.exception(e)

    return res, "200 OK"


def content_ids(params):
    """does the same this as `pageviews`, except it includes content ids and then optionally filters
    the response by a list of content ids passed as query params - note, this load can be a little
    heavy and could take a minute
    """
    # set up default values
    default_from, default_to, yesterday, _ = make_default_times()

    # get params
    try:
        series = params.get("site", [DEFAULT_SERIES])[0]
        from_date = params.get("from", [default_from])[0]
        to_date = params.get("to", [default_to])[0]
        group_by = params.get("group_by", [DEFAULT_GROUP_BY])[0]
        ids = params.get("content_id", [])
    except Exception as e:
        LOGGER.exception(e)
        return json.dumps({"error": e.message}), "500 Internal Error"

    # check the cache
    cache_key = "{}:{}:{}:{}:{}:{}:{}".format(
        memcached_prefix, "contentids.json", series, from_date, to_date, group_by, ids)
    try:
        data = MEMCACHED_CLIENT.get(cache_key)
        if data:
            return data, "200 OK"
    except Exception as e:
        LOGGER.exception(e)

    # enforce content ids
    if not len(ids):
        return json.dumps({"error": "you must pass at least one content id'"}), "400 Bad Request"

    # parse from date
    from_date = parse_datetime(from_date)
    if from_date is None:
        return json.dumps({"error": "could not parse 'from'"}), "400 Bad Request"

    # parse to date
    to_date = parse_datetime(to_date)
    if to_date is None:
        return json.dumps({"error": "could not parse 'to'"}), "400 Bad Request"

    # influx will only keep non-aggregated data for a day, so if the from param is beyond that point
    # we need to update the series name to use the rolled up values
    if from_date < yesterday:
        series = update_series(series)

    # format times
    from_date = format_datetime(from_date)
    to_date = format_datetime(to_date)

    # start building the query
    query = "SELECT content_id, sum(value) as value " \
            "FROM {series} " \
            "WHERE time > '{from_date}' AND time < '{to_date}' " \
            "GROUP BY content_id, time({group_by}) " \
            "fill(0);"
    args = {"series": series, "from_date": from_date, "to_date": to_date, "group_by": group_by}

    # send the request
    try:
        res = INFLUXDB_CLIENT.query(query.format(**args))

    # capture errors and send them back along with the query (for inspection/debugging)
    except Exception as e:
        LOGGER.exception(e)
        return json.dumps({"error": e.message, "query": query.format(**args)}), "500 Internal Error"

    # build the response object
    response = flatten_response(res)

    # filter by content ids
    if len(ids):
        for site, points in response.items():
            filtered = filter(lambda p: p["content_id"] in ids, points)
            response[site] = filtered
    res = json.dumps(response)

    # cache the response
    try:
        MEMCACHED_CLIENT.set(cache_key, res, time=MEMCACHED_EXPIRATION)
    except Exception as e:
        LOGGER.exception(e)

    return res, "200 OK"


def trending(params):
    """gets trending content values
    """
    # get params
    try:
        series = params.get("site", [DEFAULT_SERIES])[0]
        offset = params.get("offset", [DEFAULT_GROUP_BY])[0]
        limit = params.get("limit", [20])[0]
    except Exception as e:
        LOGGER.exception(e)
        return json.dumps({"error": e.message}), "500 Internal Error"

    # check the cache
    cache_key = "{}:{}:{}:{}:{}".format(memcached_prefix, "trending.json", series, offset, limit)
    try:
        data = MEMCACHED_CLIENT.get(cache_key)
        if data:
            return data, "200 OK"
    except Exception as e:
        LOGGER.exception(e)

    # update series name
    series = update_trending_series(series)

    # parse the limit
    try:
        limit = int(limit)
    except ValueError:
        return json.dumps({"error": "limit param must be an integer"}), "400 Bad Request"

    # build the query
    query = "SELECT content_id, sum(value) as value " \
            "FROM {series} " \
            "WHERE time > now() - {offset} " \
            "GROUP BY content_id;"
    args = {"series": series, "offset": offset}

    # send the request
    try:
        res = INFLUXDB_CLIENT.query(query.format(**args))

    # capture errors and send them back along with the query (for inspection/debugging)
    except Exception as e:
        LOGGER.exception(e)
        return json.dumps({"error": e.message, "query": query.format(**args)}), "500 Internal Error"

    # build the response object
    response = flatten_response(res)

    # limit the number of content per site
    for site, points in response.items():
        sorted_content = sorted(points, key=lambda p: p["value"], reverse=True)[:limit]
        response[site] = sorted_content

    clean_response = {}
    for site, values in response.items():
        clean_name = site.split("-")[0]
        clean_response[clean_name] = values
    res = json.dumps(clean_response)

    # cache the response
    try:
        MEMCACHED_CLIENT.set(cache_key, res, time=MEMCACHED_EXPIRATION)
    except Exception as e:
        LOGGER.exception(e)

    return res, "200 OK"


def videoplays(params):
    """takes a couple (optional) query parameters and queries influxdb and sends a modified response
    """
    # set up default values
    default_from, default_to, yesterday, _ = make_default_times()

    # get params
    try:
        series = "onionstudios"
        from_date = params.get("from", [default_from])[0]
        to_date = params.get("to", [default_to])[0]
        group_by = params.get("group_by", [DEFAULT_GROUP_BY])[0]
    except Exception as e:
        LOGGER.exception(e)
        return json.dumps({"error": e.message}), "500 Internal Error"

    # check the cache
    cache_key = "{}:{}:{}:{}:{}:{}".format(memcached_prefix, "videoplays.json", series, from_date, to_date, group_by)
    try:
        data = MEMCACHED_CLIENT.get(cache_key)
        if data:
            return data, "200 OK"
    except Exception as e:
        LOGGER.exception(e)

    # parse from date
    from_date = parse_datetime(from_date)
    if from_date is None:
        return json.dumps({"error": "could not parse 'from'"}), "400 Bad Request"

    # parse to date
    to_date = parse_datetime(to_date)
    if to_date is None:
        return json.dumps({"error": "could not parse 'to'"}), "400 Bad Request"

    # influx will only keep non-aggregated data for a day, so if the from param is beyond that point
    # we need to update the series name to use the rolled up values
    if from_date < yesterday:
        series = update_series(series)

    # format times
    from_date = format_datetime(from_date)
    to_date = format_datetime(to_date)

    # build out the query
    query = "SELECT sum(value) as value " \
            "FROM {series} " \
            "WHERE time > '{from_date}' " \
            "AND time < '{to_date}' " \
            "AND event =~ /^videoplay$/ " \
            "GROUP BY time({group_by}) " \
            "fill(0);"
    args = {"series": series, "from_date": from_date, "to_date": to_date, "group_by": group_by}

    # send the request
    try:
        res = INFLUXDB_CLIENT.query(query.format(**args))

    # capture errors and send them back along with the query (for inspection/debugging)
    except Exception as e:
        LOGGER.exception(e)
        return json.dumps({"error": e.message, "query": query.format(**args)}), "500 Internal Error"

    # build the response object
    response = flatten_response(res)
    res = json.dumps(response)

    # cache the response
    try:
        MEMCACHED_CLIENT.set(cache_key, res, time=MEMCACHED_EXPIRATION)
    except Exception as e:
        LOGGER.exception(e)

    return res, "200 OK"


def embedlays(params):
    """takes a couple (optional) query parameters and queries influxdb and sends a modified response
    """
    # set up default values
    default_from, default_to, yesterday, _ = make_default_times()

    # get params
    try:
        series = "onionstudios"
        from_date = params.get("from", [default_from])[0]
        to_date = params.get("to", [default_to])[0]
        group_by = params.get("group_by", [DEFAULT_GROUP_BY])[0]
    except Exception as e:
        LOGGER.exception(e)
        return json.dumps({"error": e.message}), "500 Internal Error"

    # check the cache
    cache_key = "{}:{}:{}:{}:{}:{}".format(memcached_prefix, "embedplays.json", series, from_date, to_date, group_by)
    try:
        data = MEMCACHED_CLIENT.get(cache_key)
        if data:
            return data, "200 OK"
    except Exception as e:
        LOGGER.exception(e)

    # parse from date
    from_date = parse_datetime(from_date)
    if from_date is None:
        return json.dumps({"error": "could not parse 'from'"}), "400 Bad Request"

    # parse to date
    to_date = parse_datetime(to_date)
    if to_date is None:
        return json.dumps({"error": "could not parse 'to'"}), "400 Bad Request"

    # influx will only keep non-aggregated data for a day, so if the from param is beyond that point
    # we need to update the series name to use the rolled up values
    if from_date < yesterday:
        series = update_series(series)

    # format times
    from_date = format_datetime(from_date)
    to_date = format_datetime(to_date)

    # build out the query
    query = "SELECT sum(value) as value " \
            "FROM {series} " \
            "WHERE time > '{from_date}' " \
            "AND time < '{to_date}' " \
            "AND event =~ /^embedplay$/ " \
            "GROUP BY time({group_by}) " \
            "fill(0);"
    args = {"series": series, "from_date": from_date, "to_date": to_date, "group_by": group_by}

    # send the request
    try:
        res = INFLUXDB_CLIENT.query(query.format(**args))

    # capture errors and send them back along with the query (for inspection/debugging)
    except Exception as e:
        LOGGER.exception(e)
        return json.dumps({"error": e.message, "query": query.format(**args)}), "500 Internal Error"

    # build the response object
    response = flatten_response(res)
    res = json.dumps(response)

    # cache the response
    try:
        MEMCACHED_CLIENT.set(cache_key, res, time=MEMCACHED_EXPIRATION)
    except Exception as e:
        LOGGER.exception(e)

    return res, "200 OK"


# create the wait loop
gevent.spawn(count_events)


# main applications
def application(env, start_response):
    """wsgi application
    """
    path = env["PATH_INFO"]

    if path == "/influx.gif":
        # send response
        start_response("200 OK", [("Content-Type", "image/gif")])
        yield GIF
        # parse the query params and stick them in the queue
        params = parse_qs(env["QUERY_STRING"])
        try:
            site = params.get("site", [""])[0]
            content_id = params.get("content_id", [""])[0]
            event = params.get("event", [""])[0]
            path = params.get("path", [""])[0]
            EVENTS_QUEUE.put((site, content_id, event, path))
        except Exception as e:
            LOGGER.exception(e)

    elif path == "/pageviews.json":
        params = parse_qs(env["QUERY_STRING"])
        data, status = pageviews(params)
        start_response(status, [("Content-Type", "application/json")])
        yield data

    elif path == "/embedviews.json":
        params = parse_qs(env["QUERY_STRING"])
        data, status = embedviews(params)
        start_response(status, [("Content-Type", "application/json")])
        yield data

    elif path == "/contentids.json":
        params = parse_qs(env["QUERY_STRING"])
        data, status = content_ids(params)
        start_response(status, [("Content-Type", "application/json")])
        yield data

    elif path == "/trending.json":
        params = parse_qs(env["QUERY_STRING"])
        data, status = trending(params)
        start_response(status, [("Content-Type", "application/json")])
        yield data

    elif path == "/videoplays.json":
        params = parse_qs(env["QUERY_STRING"])
        data, status = videoplays(params)
        start_response(status, [("Content-Type", "application/json")])
        yield data

    elif path == "/embedplays.json":
        params = parse_qs(env["QUERY_STRING"])
        data, status = embedlays(params)
        start_response(status, [("Content-Type", "application/json")])
        yield data

    else:
        start_response("404 Not Found", [("Content-Type", "text/plain")])
        yield "Nothing Here"
