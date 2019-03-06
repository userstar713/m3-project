from typing import List, Iterable
from sqlalchemy import func

from celery import Celery
from application import config
from application.db_extension.models import db, PipelineSequence
from application.logging import logger

from .processor import (
    ProductProcessor,
    Product,
    UpdateProduct,
    clean_sources
)

from .pipeline import execute_pipeline


celery = Celery(
    'scraping',
    autofinalize=False,
    broker=config.CELERY_BROKER_URL,
    backend=config.CELERY_RESULT_BACKEND
)

def prepare_products(source_id: int, products: Iterable,
                     full=True) -> List[dict]:
    if full:
        res = [Product.from_raw(source_id, product).as_dict() for product in
               products]
    else:
        [p.update({'source_id': source_id}) for p in products]
        res = products
    return res


@celery.task(bind=True)
def process_product_list_task(_, chunk: List[dict], full=True) -> tuple:
    """
    Process list of products in ProductProcessor
    :param chunk:
    :return:
    """
    if not chunk:
        return [], False

    logger.info("Processing %s products", len(chunk))
    source_id = chunk[0]['source_id']
    processor = ProductProcessor(source_id, full=full)
    if not full:
        processor.set_original_values()
    add_new_products = False
    products = []
    for i, product in enumerate(chunk):
        logger.info("Processing product # %s", i)
        if not full and len(product) > 5:
            product = Product.from_raw(source_id, product).as_dict()
            p = Product(**product)
            processor.process(p)
            add_new_products = True
        elif full:
            p = Product(**product)
            processor.process(p)
        else:
            p = UpdateProduct(**product)
            processor.update_product(p)
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
    if products and (full or new_products):
        sequence_id = db.session.query(
            func.max(PipelineSequence.id)
        ).filter(
            PipelineSequence.source_id == source_id
        ).scalar()
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
    if not products:
        logger.info('No products scraped for the source_id=%s', source_id)
    else:
        from application.db_extension.dictionary_lookup.lookup import dictionary_lookup
        dictionary_lookup.update_dictionary_lookup_data()
    return prepare_products(source_id, products, full=full)


def queue_sequence(source_id: int):
    """Set the state of the related row in pipeline_sequence to 'queued'."""
    sequence = db.session.query(PipelineSequence).filter(
        PipelineSequence.source_id == source_id
    ).first()
    if sequence:
        sequence.state = 'queued'
        db.session.commit()


@celery.task(bind=True)
def start_synchronization_task(_, source_id: int, full=True):
    start_synchronization(source_id, full=full)


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
