import logging
import pickle

from application.caching import cache

logger = logging.getLogger(__name__)

def get_products_from_redis(source_id, full=True):
    if full:
        redis_key = f'spider::{source_id}::data'
    else:
        redis_key = f'spider::{source_id}::data::inc'
    pickled_products = cache.get(redis_key)
    products = []
    try:
        products = pickle.loads(pickled_products)
    except BaseException:
        logger.warning(f'Error reading products for the source: {source_id}')
    return products