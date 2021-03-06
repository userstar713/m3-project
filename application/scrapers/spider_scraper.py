import io
import json

from billiard import Process
from tempfile import NamedTemporaryFile
from typing import List
from scrapy import signals
from scrapy.crawler import Crawler
from scrapy.spiders import Spider

from twisted.internet import reactor
from application.config import (SCRAPER_PAGES_LIMIT,
                                PROXY_URL,
                                DISABLE_CLIENT_CONTEXTFACTORY,
                                )
from application.spiders.base.abstracts.spider import (
    CONCURRENT_REQUESTS,
    COOKIES_DEBUG,
    DOWNLOADER_CLIENTCONTEXTFACTORY)


def get_spider_settings(tmp_file: io.IOBase, source_id: int, spider: Spider,
                        full_scrape=True) -> dict:
    settings = {
        'CONCURRENT_REQUESTS': CONCURRENT_REQUESTS,
        'COOKIES_DEBUG': COOKIES_DEBUG,
        'FEED_FORMAT': 'jsonlines',
        'FEED_URI': f'file://{tmp_file.name}',
        'DOWNLOADER_CLIENT_TLS_METHOD': 'TLS',
        'DOWNLOADER_CLIENTCONTEXTFACTORY': DOWNLOADER_CLIENTCONTEXTFACTORY,
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
        'CLOSESPIDER_PAGECOUNT': SCRAPER_PAGES_LIMIT,
        'FULL_SCRAPE': full_scrape,
        'SOURCE_ID': source_id,
    }
    # MB: Doesn't run on MB Mac with this. But seems to be required in production
    if DISABLE_CLIENT_CONTEXTFACTORY:
        del settings['DOWNLOADER_CLIENTCONTEXTFACTORY']

    if full_scrape:
        settings['ITEM_PIPELINES'] = {spider.filter_pipeline: 300}
    else:
        settings['ITEM_PIPELINES'] = {spider.inc_filter_pipeline: 300}
    if PROXY_URL:
        settings.update({
            'DOWNLOADER_MIDDLEWARES': {
                'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
                'scrapy_proxies.RandomProxy': 100,
                'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 110,
            },
            'RETRY_TIMES': 10,
            'RETRY_HTTP_CODES': [500, 502, 503, 504, 400, 403, 404, 408],
            'PROXY_MODE': 2,
            'CUSTOM_PROXY': PROXY_URL,
        })
    return settings


class CrawlerScript(Process):
    def __init__(self, tmp_file, source_id, spider, full):
        Process.__init__(self)
        settings = get_spider_settings(
            tmp_file, source_id, spider, full_scrape=full
        )
        self.crawler = Crawler(spider, settings)
        self.crawler.signals.connect(
            reactor.stop, signal=signals.spider_closed)
        self.spider = spider

    def run(self):
        self.crawler.crawl()
        reactor.run()


class SpiderScraper():
    def __init__(self, spider_cls):
        self._spider_cls = spider_cls

    def run(self, source_id: int, full=True) -> List[dict]:
        with NamedTemporaryFile() as f:
            crawler = CrawlerScript(f, source_id, self._spider_cls, full)
            crawler.start()
            crawler.join()
            return [json.loads(line) for line in f]
