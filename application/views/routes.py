from flask import (jsonify,
                   request,
                   Response)

from application.tasks.spiders import execute_spider
from application.tasks.synchronization import execute_pipeline_task

from .helpers import seller_integration_bp

from ..tasks import start_synchronization


@seller_integration_bp.route(
    '/status'
)
def index():
    return jsonify(
        {'status': 'ok'}
    )


@seller_integration_bp.route(
    '/source/<int:source_id>/sync',
    strict_slashes=False
)
def sync(source_id: int) -> Response:
    # content = request.get_json()
    # callback_url = content['callback_url']
    try:
        full = request.args.get('full', 1)
        full = bool((int(full)))
    except TypeError:
        full = False
    update_lookup_data()
    status = start_synchronization(source_id, full=full)
    return jsonify(
        {'data': {
            'status': status,
            # 'callback_url': callback_url
        }
        }
    )


@seller_integration_bp.route(
    '/source/<int:source_id>/scrape/',
)
def scrape(source_id: int) -> Response:
    status = execute_spider(source_id)
    return jsonify(
        {
            'data': {
                'status': status,
            }
        }
    )

def update_lookup_data():
    from application.db_extension.dictionary_lookup.lookup import dictionary_lookup
    dictionary_lookup.update_dictionary_lookup_data()

@seller_integration_bp.route(
    '/source/<int:source_id>/execute_pipeline',
)
def execute_pipeline(source_id: int) -> Response:
    update_lookup_data()
    task = execute_pipeline_task.delay(([True], True), source_id)
    return jsonify(
        {'data': {'status': task.id}}
    )
