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
CACHE_REDIS_HOST = getenv('REDIS_HOST',
                          'redis')
CACHE_REDIS_DB = int(getenv('REDIS_DB', 1))
CACHE_REDIS_PORT = int(getenv('REDIS_PORT', 6379))

BASE_PATH = Path(__file__).parent

SQLALCHEMY_DATABASE_URI = getenv('SQLALCHEMY_DATABASE_URI',
                                 'postgresql://'
                                 'postgres:password@db:5432/m3')

# https://github.com/mitsuhiko/flask-sqlalchemy/issues/365
SQLALCHEMY_TRACK_MODIFICATIONS = False

CELERY_BROKER_URL = getenv('CELERY_BROKER_URL',
                           'redis://redis:6379/1')

CELERY_RESULT_BACKEND = getenv('CELERY_RESULT_BACKEND',
                               'redis://redis:6379/1')
