import io
import json

from tempfile import NamedTemporaryFile
from typing import List
from scrapy.crawler import CrawlerRunner

from twisted.internet import reactor
from application.config import (SCRAPER_PAGES_LIMIT,
                                PROXY_URL,
                                )
from application.spiders.base.abstracts.spider import (
    CONCURRENT_REQUESTS,
    COOKIES_DEBUG,
    DOWNLOADER_CLIENTCONTEXTFACTORY)
from .base import BaseScraper


def get_spider_settings(tmp_file: io.IOBase) -> dict:
    settings = {
        'CONCURRENT_REQUESTS': CONCURRENT_REQUESTS,
        'COOKIES_DEBUG': COOKIES_DEBUG,
        'FEED_FORMAT': 'jsonlines',
        'FEED_URI': f'file://{tmp_file.name}',
        'DOWNLOADER_CLIENT_METHOD': 'TLSv1.2',
        'DOWNLOADER_CLIENTCONTEXTFACTORY': DOWNLOADER_CLIENTCONTEXTFACTORY,
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
        'CLOSESPIDER_PAGECOUNT': SCRAPER_PAGES_LIMIT,
    }
    if PROXY_URL:
        settings.update({
            'DOWNLOADER_MIDDLEWARES': {
                'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
                'scrapy_proxies.RandomProxy': 100,
                'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 110,
            },
            'RETRY_TIMES': 10,
            'RETRY_HTTP_CODES': [500, 503, 504, 400, 403, 404, 408],
            'PROXY_MODE': 2,
            'CUSTOM_PROXY': PROXY_URL,
        })
    return settings

def run_spider(spider_cls, tmp_file):
    settings = get_spider_settings(tmp_file)
    runner = CrawlerRunner(settings)
    deferred = runner.crawl(spider_cls)
    deferred.addBoth(lambda _: reactor.stop())
    reactor.run()


class SpiderScraper(BaseScraper):
    def __init__(self, spider_cls):
        self._spider_cls = spider_cls

    def run(self) -> List[dict]:
        with NamedTemporaryFile() as f:
            run_spider(self._spider_cls, f)
            return [json.loads(line) for line in f]

