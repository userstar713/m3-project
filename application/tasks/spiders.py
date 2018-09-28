import json
import pickle
from tempfile import NamedTemporaryFile

from application.tasks.synchronization import celery
from application.spiders import klwines
from application.db_extension.models import db, Source
from application.caching import cache
from scrapy.crawler import Crawler
from billiard import Process

class UrlCrawlerScript(Process):
    def __init__(self, spider):
        Process.__init__(self)
        self.spider = spider
        self.crawler = Crawler()
        self.crawler.configure()
        # self.crawler.signals.connect(reactor.stop, signal=signals.spider_closed)


    def run(self):
        self.crawler.crawl(self.spider)
        self.crawler.start()
        # reactor.run()


def run_spider(spider_cls):
    spider = spider_cls()
    crawler = UrlCrawlerScript(spider)
    crawler.start()
    crawler.join()

def execute_spider(source_id: int) -> str:
    task = task_execute_spider.delay(source_id)
    return task.id

@celery.task(bind=True)
def task_execute_spider(self, source_id: int) -> None:
    source = db.session.query(Source).get(source_id)
    if source.name == 'K&L Wines':
        spider_cls = klwines.KLWinesSpider
    else:
        raise ValueError(f'No spider with name {source.name}')
    with NamedTemporaryFile() as f:
        run_spider(spider_cls)
        data = [json.loads(line) for line in f]
        cache.set(f'spider::{source_id}::data', pickle.dumps(data, protocol=-1))