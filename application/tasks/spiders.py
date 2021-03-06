import pickle
import logging


from typing import List
from flask import current_app
from application.tasks.synchronization import celery
from application.spiders import (klwines,
                                 thewineclub,
                                 totalwine,
                                 wine_com,
                                 wine_library,
                                 bevsites)
from application.db_extension.models import db, Source

from application.scrapers import SpiderScraper

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
def task_execute_spider(self, source_id: int, full=True) -> None:
    source = db.session.query(Source).get(source_id)

    # TODO refactor that
    if source.source_code == 'KLWINES':
        scraper = SpiderScraper(klwines.KLWinesSpider)
    elif source.source_code == 'WINELIBRARY':
        scraper = SpiderScraper(wine_library.WineLibrarySpider)
    elif source.source_code == 'WINECOM':
        scraper = SpiderScraper(wine_com.WineComSpider)
    elif source.source_code == 'THEWINECLUB':
        scraper = SpiderScraper(thewineclub.TheWineClubSpider)
    elif source.source_code == 'TOTALWINE':
        scraper = SpiderScraper(totalwine.TotalWineSpider)
    elif source.source_code == 'BEVSITES':
        scraper = bevsites.BevsitesCSVScraper()
    else:
        raise ValueError(f'No support for source with name {source.name}')
    
    data = scraper.run(source_id, full=full)
    
    
    return data
