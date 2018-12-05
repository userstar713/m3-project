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
    '/source/<int:source_id>/sync/',
)
def sync(source_id: int) -> Response:
    # content = request.get_json()
    # callback_url = content['callback_url']
    try:
        full = request.args.get('full', 1)
        full = bool((int(full)))
    except TypeError:
        full = False
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


@seller_integration_bp.route(
    '/source/<int:source_id>/execute_pipeline/',
)
def execute_pipeline(source_id: int) -> Response:
    status = execute_pipeline_task.si(source_id)
    return jsonify(
        {'data': {'status': 'ok'}}
    )
