from flask import (jsonify,
                   request,
                   Response)

from application.tasks.spiders import execute_spider
from application.tasks.synchronization import execute_pipeline_task
from application.db_extension.routines import python_dictionary_lookup

from .helpers import seller_integration_bp

from ..tasks import start_synchronization


@seller_integration_bp.route('/lookup_attributes', methods=['POST'])
def route_lookup_attributes():

    # Chew up some memory
    mymem = "X" * 20000000  # 20m

    body = request.get_json()
    sentence = body.get('text', '')
    # category_id = int(body.get('category_id', 1))
    attr_codes = body.get('attr_codes')
    result = python_dictionary_lookup(None, sentence, attr_codes)
    print(result)
    return jsonify(result)


@seller_integration_bp.route('/reload_dictionary')
def route_reload_dictionary():
    from application.db_extension.dictionary_lookup.lookup import dictionary_lookup
    dictionary_lookup.update_dictionary_lookup_data()
    return jsonify({'msg': 'dictionary reloaded'})


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
    '/source/<int:source_id>/execute_pipeline',
)
def execute_pipeline(source_id: int) -> Response:
    task = execute_pipeline_task.delay(([True], True), source_id)
    return jsonify(
        {'data': {'status': task.id}}
    )
