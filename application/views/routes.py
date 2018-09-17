from flask import jsonify, request, Response

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
    methods=['POST']
)
def sync(source_id: int) -> Response:
    content = request.get_json()
    callback_url = content['callback_url']
    status = start_synchronization(source_id)
    return jsonify(
        {'data': {
            'status': status,
            'callback_url': callback_url
            }
        }
    )
