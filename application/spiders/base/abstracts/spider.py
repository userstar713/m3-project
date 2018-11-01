from abc import ABC, abstractmethod
from scrapy.spiders import Spider

CONCURRENT_REQUESTS = 16
COOKIES_DEBUG = False
DOWNLOADER_CLIENTCONTEXTFACTORY = ('application.spiders.base.cipher_factory.'
                                   'CustomCipherContextFactory')


class AbstractSpider(ABC, Spider):

    @abstractmethod
    def start_requests(self):
        pass

    @abstractmethod
    def before_login(self, response):
        pass

    @abstractmethod
    def login(self, response):
        pass

    @abstractmethod
    def is_not_logged(self, response):
        pass

    def get_wine_types(self, response):
        pass

    @abstractmethod
    def get_listpages(self, response):
        pass

    def parse_wine_types(self, response):
        pass

    @abstractmethod
    def parse_listpage(self, response):
        pass

    @abstractmethod
    def parse_product(self, response):
        pass
