from flask import jsonify, request, Response

from .helpers import seller_integration_bp

from ..tasks import start_synchronization

from application.tasks.spiders import execute_spider


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
    status = start_synchronization(source_id)
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
