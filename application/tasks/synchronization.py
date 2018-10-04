import pickle
from typing import List, Iterable

from celery import Celery
from flask import current_app

from application.logging import logger
from sqlalchemy import func
from application.db_extension.models import db, PipelineSequence, Source
from application.caching import cache
from .processor import ProductProcessor, Product
from .pipeline import execute_pipeline


celery = Celery(__name__, autofinalize=False)

@celery.task(bind=True)
def add(_, x, y):
    return x + y


@celery.task(bind=True)
def sample_task(_):
    '''sample task that sleeps 5 seconds then returns the current datetime'''
    import time, datetime
    time.sleep(5)
    return datetime.datetime.now().isoformat()

def get_test_products() -> List:
    import csv
    root = current_app.config['BASE_PATH']
    with open(root / 'tools/products.csv', 'r') as f:
        rdr = csv.DictReader(f)
        result = [r for r in rdr]
    logger.debug(f"Got {len(result)} test products")
    return result

def prepare_products(source_id: int, products: Iterable) -> List[dict]:
    return [Product.from_raw(source_id, product).as_dict() for product in
            products]


@celery.task(bind=True, name='tasks.process_product_list')
def process_product_list_task(_, chunk: List[dict]) -> None:
    """
    Process list of products in ProductProcessor
    :param chunk:
    :return:
    """
    logger.debug(f"Processing {len(chunk)} products")
    processor = ProductProcessor()
    for product in chunk:
        p = Product(**product)
        processor.process(p)
    processor.flush()

@celery.task(bind=True)
def execute_pipeline_task(_, source_id: int) -> None :
    """
    Final part of the pipeline processing
    :param source_id:
    :return:
    """
    sequence_id = db.session.query(func.max(PipelineSequence.id)).filter(
        PipelineSequence.source_id == source_id
    ).scalar()

    execute_pipeline(source_id, sequence_id)


@celery.task(bind=True)
def get_products_task(_, source_id: int) -> List[dict]:
    """
    Get raw data from the redis and return it as dictionaries
    :param source_id:
    :return:
    """
    redis_key = f'spider::{source_id}::data'
    pickled_products = cache.get(redis_key)
    products = pickle.loads(pickled_products)
    # return list(chunkify(prepare_products(source_id, products), 500))
    return prepare_products(source_id, products)

def start_synchronization(source_id: int) -> str:
    from .spiders import task_execute_spider
    logger.info(f'Starting synchronization for source: {source_id}')

    if source_id == 12345:
        # override id 12345 for test purposes
        redis_key = f'spider::{source_id}::data'
        data = get_test_products()[:1000]
        cache.set(redis_key, pickle.dumps(data, protocol=-1))
        is_use_interim = True # don't run scraper
    else:
        source = db.session.query(Source).get(source_id)
        is_use_interim = source.is_use_interim

    # if "is_use_interim" is not set, run a full sequence (with scraping)
    # if it is set, don't run the scraper, use the data from
    pipeline_job = get_products_task.si(source_id) | process_product_list_task.s() \
                   | execute_pipeline_task.si(source_id)
    if is_use_interim:
        job = pipeline_job
    else:
        job = task_execute_spider.si(source_id) | pipeline_job
    logger.info('Calling job.delay()')
    task = job.delay()
    return task.id
