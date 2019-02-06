from celery.schedules import crontab
from application.db_extension.models import db
from .synchronization import start_synchronization, celery


def init_celery(app):

    # set broker url and result backend from app config
    celery.conf.broker_url = app.config['CELERY_BROKER_URL']
    celery.conf.result_backend = app.config['CELERY_RESULT_BACKEND']
    # restarts the worker process after each task to prevent out of memory
    celery.conf.worker_concurrency = 10
    # celery.conf.worker_max_tasks_per_child = 1
    if app.config['CELERY_EAGER']:
        celery.conf.task_always_eager = True
        celery.conf.task_eager_propagates = True
    # subclass task base for app context
    # http://flask.pocoo.org/docs/0.12/patterns/celery/
    TaskBase = celery.Task
    celery.conf.task_routes = {'application.tasks.*': {'queue': 'scraping'}}
    base_task_name = 'application.tasks.synchronization.start_synchronization_task'
    celery.conf.beat_schedule = {
        'full_klwines_sync': {
            'task': base_task_name,
            'schedule': crontab(hour='22', minute=0),
            'options': {'queue': 'scraping'},
            'args': (1, True)},
        'full_wine_library_sync': {
            'task': base_task_name,
            'schedule': crontab(hour='22', minute=0),
            'options': {'queue': 'scraping'},
            'args': (2, True)},
        'full_wine_com_sync': {
            'task': base_task_name,
            'schedule': crontab(hour='22', minute=0),
            'options': {'queue': 'scraping'},
            'args': (3, True)},
        'full_wine_club_sync': {
            'task': base_task_name,
            'schedule': crontab(hour='22', minute=0),
            'options': {'queue': 'scraping'},
            'args': (4, True)},
        'full_totalwine_sync': {
            'task': base_task_name,
            'schedule': crontab(hour='22', minute=0),
            'options': {'queue': 'scraping'},
            'args': (5, True)},
    }


    class AppContextTask(TaskBase):
        abstract = True
        def __call__(self, *args, **kwargs):
            with app.app_context():
                try:
                    response = TaskBase.__call__(self, *args, **kwargs)
                finally:
                    # scoped session is being shared between celery and
                    # web worker, force celery to recreate the new
                    # session object on the each task
                    db.session.remove()
                return response
    celery.Task = AppContextTask

    # run finalize to process decorated tasks
    celery.finalize()
