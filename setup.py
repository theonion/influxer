import codecs
from distutils.core import setup
import os

from influxer import __version__ as version, __authors__ as authors


def get_authors():
    return ', '.join(author['name'] for author in authors)


def get_author_emails():
    return ', '.join(author['email'] for author in authors)


def get_dependencies():
    filename = os.path.join(os.path.dirname(__file__), 'requirements.txt')
    with codecs.open(filename, 'r', encoding='utf8') as fh:
        dependencies = [line.strip() for line in fh]
    return dependencies


setup(
    name='influxer',
    version=version,
    description='A small pixel tracker built in python and designed to send data to InfluxDB',
    author=get_authors(),
    author_email=get_author_emails(),
    packages=['influxer'],
    install_requires=get_dependencies(),
    url='https://github.com/theonion/influxer'
)
