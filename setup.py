from distutils.core import setup


setup(
    name='influxer',
    version='1.0.1',
    description='A small pixel tracker to send data to InfluxDB and replace TinyTracker',
    author='Vince Forgione',
    author_email='vforgione@theonion.com',
    packages=['influxer'],
    install_requires=[
        'gevent==1.0.1',
        'influxdb==0.1.13',
    ],
    url='https://github.com/theonion/influxer'
)
