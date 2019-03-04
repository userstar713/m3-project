"""Configuration parameters module"""
from os import getenv
from pathlib import Path

DEBUG = bool(getenv('DEBUG', False))

#
#   Cache settings (required by Flask)
#

CACHE_TYPE = getenv('CACHE_TYPE',
                    'redis')  # memcached, redis... https://pythonhosted.org/Flask-Caching/

#
# Redis settings (Flask-Caching)
#
REDIS_HOST = getenv('REDIS_HOST', 'redis://redis:6379')
assert REDIS_HOST.startswith('redis://')
REDIS_DB = int(getenv('REDIS_DB', 1))


REDIS_URI = f'{REDIS_HOST}/{REDIS_DB}'

CACHE_REDIS_HOST, CACHE_REDIS_PORT = REDIS_HOST.replace('redis://','').split(':')
CACHE_REDIS_DB = REDIS_DB

BASE_PATH = Path(__file__).parent

SQLALCHEMY_DATABASE_URI = getenv('SQLALCHEMY_DATABASE_URI',
                                 'postgresql://'
                                 'postgres:password@db:5432/m3')

# https://github.com/mitsuhiko/flask-sqlalchemy/issues/365
SQLALCHEMY_TRACK_MODIFICATIONS = False

CELERY_BROKER_URL = getenv('CELERY_BROKER_URL', REDIS_URI)

CELERY_RESULT_BACKEND = getenv('CELERY_RESULT_BACKEND', REDIS_URI)

CELERY_EAGER = getenv('CELERY_EAGER', 0)
SCRAPER_PAGES_LIMIT = int(getenv('SCRAPER_PAGES_LIMIT', 0))
PROXY_URL = 'http://lum-customer-hl_2a4458fe-zone-static-country-us:zidoxuxldfjn@zproxy.lum-superproxy.io:22225'