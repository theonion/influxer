from distutils.core import setup


setup(
    name='influxer',
    version='0.0.1',
    description='A small pixel tracker built in python and designed to send data to InfluxDB',
    author='Vince Forgione',
    author_email='vforgione@theonion.com',
    packages=['influxer'],
    install_requires=[
        'influxdb==0.1.13'
    ],
    url='https://github.com/theonion/influxer'
)
