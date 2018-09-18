# project/__init__.py
from flask import Flask
from .views import init_app as init_views
from .tasks import celery, init_celery
from application.caching import cache
from application.db_extension.models import db
from application.db_extension.routines import configure_default_category_id
from .logging import logger

def create_app() -> Flask:
    return entrypoint(mode='app')

def create_celery() -> Flask:
    return entrypoint(mode='celery')

def entrypoint(mode: str = 'app') -> Flask:
    app = Flask(__name__)

    app.config.from_pyfile('config.py')
    app.debug = app.config['DEBUG']
    if app.debug:
        logger.info('Application running in DEBUG mode')
        logger.info(f"SQLALCHEMY_DATABASE_URI={app.config['SQLALCHEMY_DATABASE_URI']}")
    db.init_app(app)
    with app.app_context():
        configure_default_category_id()
    init_views(app)
    init_celery(app)
    cache.init_app(app)


    if mode == 'app':
        return app
    elif mode == 'celery':
        return celery
