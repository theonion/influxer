from distutils.core import setup


setup(
    name="influxer",
    version="1.0.16",
    description="Read and Write Event Data to InfluxDB",
    author="Vince Forgione",
    author_email="vforgione@theonion.com",
    packages=["influxer"],
    install_requires=[
        "gevent==1.0.1",
        "influxdb==0.1.13",
        "pylibmc==1.4.2",
    ],
    url="https://github.com/theonion/influxer"
)
