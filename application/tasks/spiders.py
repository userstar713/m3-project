import pickle
import logging


from typing import List
from flask import current_app
from application.tasks.synchronization import celery
from application.spiders import (klwines,
                                 wine_com,
                                 wine_library)
from application.db_extension.models import db, Source
from application.caching import cache

from application.scrapers import SpiderScraper, CSVURLScraper
from .common import get_products_from_redis


LOGGER = logging.getLogger(__name__)


def execute_spider(source_id: int) -> str:
    task = task_execute_spider.delay(source_id)
    return task.id


def get_test_products() -> List:
    import csv
    root = current_app.config['BASE_PATH']
    with open(root / 'tools/products.csv', 'r') as f:
        rdr = csv.DictReader(f)
        result = [r for r in rdr]
    LOGGER.debug(f"Got {len(result)} test products")
    return result


@celery.task(bind=True)
def task_execute_spider(self, source_id: int) -> None:
    source = db.session.query(Source).get(source_id)
    is_use_interim = source.is_use_interim
    is_data_exists = bool(get_products_from_redis(source_id))

    if is_use_interim and is_data_exists:
        # skip spider if we have products in the redis
        # and is_use_interim is set
        return None

    # TODO refactor that
    if source.name == 'Vinomogul':
        scraper = CSVURLScraper(
            'https://dl.dropbox.com/s/sli22wzl4i245fm/out_new_dev.csv'
        )
    elif source.name == 'K&L Wines':
        scraper = SpiderScraper(klwines.KLWinesSpider)
    elif source.name == 'Wine Library':
        scraper = SpiderScraper(wine_library.WineLibrarySpider)
    elif source.name == 'Wine.com':
        scraper = SpiderScraper(wine_com.WineComSpider)
    else:
        raise ValueError(f'No support for source with name {source.name}')

    data = scraper.run()

    cache.set(f'spider::{source_id}::data',
              pickle.dumps(data, protocol=-1),
              timeout=0)
