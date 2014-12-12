# influxer

a tiny event tracking proxy for influxdb inspired by [tinytracker](https://github.com/theonion/tinytracker) -- 
in all actuality it's a fork to work with influx and not graphite


## running with uwsgi

to run the server:

```bash
$ uwsgi --http 127.0.0.1:1337 --module influxer.wsgi:application --master -H /path/to/workspace
```

and then fire off requests to it a la:

```bash
$ curl "http://127.0.0.1:1337/influx.gif?123&event=channel.content_id
```

where `channel` is the table/shard you want to logically track things and `content_id` is the content/article/video id you're tracking
