from typing import List, Iterable

from celery import Celery
from .common import get_products_from_redis

from application.logging import logger
from sqlalchemy import func
from application.db_extension.models import db, PipelineSequence
from .processor import ProductProcessor, Product
from .pipeline import execute_pipeline


celery = Celery(__name__, autofinalize=False)

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
    for i, product in enumerate(chunk):
        p = Product(**product)
        processor.process(p)
        logger.info(f"Processing product # {i}")
        if i > 8000:  # TODO REMOVE ME!
            break
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
    products = get_products_from_redis(source_id)
    # return list(chunkify(prepare_products(source_id, products), 500))
    return prepare_products(source_id, products)


def queue_sequence(source_id: int):
    """Set the state of the related row in pipeline_sequence to 'queued'."""
    sequence = db.session.query(PipelineSequence).filter(
        PipelineSequence.source_id == source_id
    ).first()
    if sequence:
        sequence.state = 'queued'
        db.session.commit()


def start_synchronization(source_id: int) -> str:
    from .spiders import task_execute_spider
    logger.info(f'Starting synchronization for source: {source_id}')
    queue_sequence(source_id)
    # if "is_use_interim" is not set, run a full sequence (with scraping)
    # if it is set, don't run the scraper, use the data from
    job = task_execute_spider.si(source_id)\
           | get_products_task.si(source_id)\
           | process_product_list_task.s() \
           | execute_pipeline_task.si(source_id)
    logger.info('Calling job.delay()')
    task = job.delay()
    return task.id
