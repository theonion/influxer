# influxer

a tiny event tracking proxy for influxdb inspired by [tinytracker](https://github.com/theonion/tinytracker)


## running with uwsgi

to run the server:

```bash
$ uwsgi --http 127.0.0.1:1337 --module influxer.wsgi:application --master -H /path/to/workspace
```


## writing data to influxer

```bash
curl -G "http://127.0.0.1:1337/influx.gif?site=onion&content_id=123&event=pageview&path=/articles/blah-123"
```


## reading data from influxer

### pageviews by site

```bash
curl -G "http://127.0.0.1:1337/pageviews.json"
```

### pageviews by content id

```bash
curl -G "http://127.0.0.1:1337/contentids.json?content_id=123"
```

### trending content

```bash
curl -G "http://127.0.0.1:1337/trending.json"
```
