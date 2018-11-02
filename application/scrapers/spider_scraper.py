import json

from tempfile import NamedTemporaryFile
from typing import List
from scrapy.crawler import CrawlerRunner

from twisted.internet import reactor
from application.config import SCRAPER_PRODUCTS_LIMIT
from application.spiders.base.abstracts.spider import (
    CONCURRENT_REQUESTS,
    COOKIES_DEBUG,
    DOWNLOADER_CLIENTCONTEXTFACTORY)
from .base import BaseScraper


def run_spider(spider_cls, tmp_file):
    settings = {
        'CONCURRENT_REQUESTS': CONCURRENT_REQUESTS,
        'COOKIES_DEBUG': COOKIES_DEBUG,
        'FEED_FORMAT': 'jsonlines',
        'FEED_URI': f'file://{tmp_file.name}',
        'DOWNLOADER_CLIENT_METHOD': 'TLSv1.2',
        'DOWNLOADER_CLIENTCONTEXTFACTORY': DOWNLOADER_CLIENTCONTEXTFACTORY,
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'

    }
    if SCRAPER_PRODUCTS_LIMIT:
        settings['CLOSESPIDER_PAGECOUNT'] = SCRAPER_PRODUCTS_LIMIT  # TODO for testing purposes, remove this on production
    runner = CrawlerRunner(settings)
    d = runner.crawl(spider_cls)
    d.addBoth(lambda _: reactor.stop())
    reactor.run()


class SpiderScraper(BaseScraper):
    def __init__(self, spider_cls):
        self._spider_cls = spider_cls

    def run(self) -> List[dict]:
        with NamedTemporaryFile() as f:
            run_spider(self._spider_cls, f)
            return [json.loads(line) for line in f]

