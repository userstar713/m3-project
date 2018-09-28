from typing import List

from celery import Celery
from flask import current_app

from application.logging import logger
from application.utils import chunkify
from application.db_extension.models import PipelineSequence
from .processor import ProductProcessor, Product, SourceAttributeValue, SourceReview
from .pipeline import execute_pipeline

celery = Celery(__name__, autofinalize=False)

from celery import group

@celery.task(bind=True)
def add(self, x, y):
    return x + y


@celery.task(bind=True)
def sample_task(self):
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

def get_products_for_source_id(source_id: int) -> List[Product]:
    if source_id == 12345:
        products = get_test_products()
    else:
        raise NotImplementedError(
            'You only able to use test source_id 12345 yet'
        )
    return [Product.from_raw(source_id, product) for product in
            products]


@celery.task(bind=True, name='tasks.process_product_list')
def process_product_list_task(self,
                              chunk: List[Product]):
    logger.debug(f"Processing {len(chunk)} products")
    processor = ProductProcessor()
    for product in chunk:
        assert isinstance(product, Product)
        processor.process(product)
    processor.flush()

@celery.task(bind=True)
def execute_pipeline_task(self, source_id):
    sequence_id = PipelineSequence.get_latest_sequence_id(
        source_id
    )

    execute_pipeline(source_id, sequence_id)


def start_synchronization(source_id: int) -> str:
    logger.info(f'Starting synchronization for source: {source_id}')
    converted_products = get_products_for_source_id(source_id)[:100]
    #converted_products = [[pr for pr in converted_products if pr.reviews]]
    chunks = list(chunkify(converted_products, 500))
    logger.info(f'{len(chunks)} chunks of products prepared for processing')
    job = group(
        process_product_list_task.s(chunk) for chunk in chunks
    ) | execute_pipeline_task.si(source_id)
    #job = group(
    #    process_product_list_task.s(chunk) for chunk in chunks
    #)
    logger.info('Calling job.delay()')
    task = job.delay()
    return task.id
