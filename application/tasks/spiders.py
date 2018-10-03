import json
import pickle
from tempfile import NamedTemporaryFile

from application.tasks.synchronization import celery
from application.spiders import klwines
from application.db_extension.models import db, Source
from application.caching import cache
from twisted.internet import reactor
import scrapy
from scrapy.crawler import CrawlerRunner

def run_spider(spider_cls, tmp_file):
    runner = CrawlerRunner({
        'CONCURRENT_REQUESTS': klwines.CONCURRENT_REQUESTS,
        'COOKIES_DEBUG': klwines.COOKIES_DEBUG,
        'FEED_FORMAT': 'jsonlines',
        'FEED_URI': f'file://{tmp_file.name}',
        'DOWNLOADER_CLIENT_METHOD': 'TLSv1.2',
        'DOWNLOADER_CLIENTCONTEXTFACTORY': klwines.DOWNLOADER_CLIENTCONTEXTFACTORY,
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'
    })
    d = runner.crawl(spider_cls)
    d.addBoth(lambda _: reactor.stop())
    reactor.run()

def execute_spider(source_id: int) -> str:
    task = task_execute_spider.delay(source_id)
    return task.id

@celery.task(bind=True)
def task_execute_spider(self, source_id: int) -> None:
    source = db.session.query(Source).get(source_id)
    if source.name == 'K&L Wines':
        spider_cls = klwines.KLWinesSpider
        #spider_func = klwines.get_data
    else:
        raise ValueError(f'No spider with name {source.name}')
    with NamedTemporaryFile() as f:
        #spider_func(f)
        run_spider(spider_cls, f)
        data = [json.loads(line) for line in f]
        cache.set(f'spider::{source_id}::data', pickle.dumps(data, protocol=-1))