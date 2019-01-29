from typing import List, Iterable
from sqlalchemy import func

from celery import Celery

from application.db_extension.models import db, PipelineSequence
from application.tasks.pipeline import (
    execute_pipeline_full,
    execute_pipeline_inc
)
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
def process_product_list_task(_, chunk: List[dict], full=True) -> None:
    """
    Process list of products in ProductProcessor
    :param chunk:
    :return:
    """
    logger.info(f"Processing {len(chunk)} products")
    processor = ProductProcessor(full=full)
    add_new_products = False
    for i, product in enumerate(chunk):
        if full:
            p = Product(**product)
            logger.info(f"Processing product # {i}")
            processor.process(p)
        else:
            p = UpdateProduct(**product)
            logger.info(f"Updating product # {i}")
            new = processor.update_product(p)
            if new and not add_new_products:
                add_new_products = True
    if not full:
        processor.delete_old_products()
    processor.flush()
    return add_new_products


@celery.task(bind=True)
def execute_pipeline_task(_, new_products: bool,
                          source_id: int, full=True) -> dict:
    """
    Final part of the pipeline processing
    :param source_id:
    :return:
    """
    sequence_id = db.session.query(func.max(PipelineSequence.id)).filter(
        PipelineSequence.source_id == source_id
    ).scalar()
    if full or new_products:
        return execute_pipeline_full(source_id, sequence_id)
    return execute_pipeline_inc(source_id, sequence_id)


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
