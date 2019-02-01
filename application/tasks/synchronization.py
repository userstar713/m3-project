from typing import List, Iterable
from sqlalchemy import func

from celery import Celery

from application.db_extension.models import db, PipelineSequence
from application.tasks.pipeline import execute_pipeline
from application.logging import logger

from .processor import (
    ProductProcessor,
    Product,
    UpdateProduct,
    clean_sources
)


celery = Celery(__name__, autofinalize=False)


def prepare_products(source_id: int, products: Iterable,
                     full=True) -> List[dict]:
    if full:
        res = [Product.from_raw(source_id, product).as_dict() for product in
               products]
    else:
        [p.update({'source_id': source_id}) for p in products]
        res = products
    return res


@celery.task(bind=True, name='tasks.process_product_list')
def process_product_list_task(_, chunk: List[dict], full=True) -> tuple:
    """
    Process list of products in ProductProcessor
    :param chunk:
    :return:
    """
    logger.info("Processing %s products", len(chunk))
    source_id = chunk[0]['source_id']
    processor = ProductProcessor(source_id, full=full)
    if not full:
        processor.set_original_values()
    add_new_products = False
    products = []
    for i, product in enumerate(chunk):
        if full:
            p = Product(**product)
            logger.info("Processing product # %s", i)
            processor.process(p)
        else:
            if len(product) > 5:
                p = Product(**product)
            else:
                p = UpdateProduct(**product)
            logger.info("Updating product # %s, %s", i, len(product))
            new = processor.update_product(p)
            if new and not add_new_products:
                add_new_products = True
        products.append(p)
    if not full:
        processor.delete_old_products()
    processor.flush()
    return products, add_new_products


@celery.task(bind=True)
def execute_pipeline_task(_, chunk: tuple, source_id: int, full=True) -> dict:
    """
    Final part of the pipeline processing
    :param source_id:
    :return:
    """
    products, new_products = chunk
    sequence_id = db.session.query(func.max(PipelineSequence.id)).filter(
        PipelineSequence.source_id == source_id
    ).scalar()
    if full or new_products:
        return execute_pipeline(source_id, sequence_id)
    return {}


@celery.task(bind=True)
def clean_sources_task(_, data: List[dict], source_id: int) -> None:
    if data:
        clean_sources(source_id)
    return data


@celery.task(bind=True)
def get_products_task(_, products: List[dict], source_id: int,
                      full=True) -> List[dict]:
    """
    Get raw data from the redis and return it as dictionaries
    :param source_id:
    :return:
    """
    return prepare_products(source_id, products, full=full)


def queue_sequence(source_id: int):
    """Set the state of the related row in pipeline_sequence to 'queued'."""
    sequence = db.session.query(PipelineSequence).filter(
        PipelineSequence.source_id == source_id
    ).first()
    if sequence:
        sequence.state = 'queued'
        db.session.commit()


def start_synchronization(source_id: int, full=True) -> str:
    from .spiders import task_execute_spider
    sync_type = full and 'Full' or 'Incremental'
    logger.info(f'Starting {sync_type} synchronization for source: {source_id}')
    queue_sequence(source_id)
    # if "is_use_interim" is not set, run a full sequence (with scraping)
    # if it is set, don't run the scraper, use the data from
    if full:
        job = task_execute_spider.si(source_id, full=full)\
            | clean_sources_task.s(source_id)\
            | get_products_task.s(source_id)\
            | process_product_list_task.s() \
            | execute_pipeline_task.s(source_id)
    else:
        job = task_execute_spider.si(source_id, full=full)\
            | get_products_task.s(source_id, full=full)\
            | process_product_list_task.s(full=full) \
            | execute_pipeline_task.s(source_id, full=full)
    logger.info('Calling job.delay()')
    task = job.delay()
    return task.id
