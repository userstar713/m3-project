from abc import ABC, abstractmethod
from typing import Iterator, Dict
from scrapy.http.response import Response
from scrapy.spiders import Spider
from application.spiders.base.wine_item import WineItem

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
    def get_product_dict(self, response: Response):
        pass

    @abstractmethod
    def get_list_product_dict(self, response: Response):
        pass

    def check_product(self, response: Response):
        res = self.get_product_dict(response)
        bottle_size = res['bottle_size']
        return bottle_size == 750 and res

    def check_list_product(self, response):
        product = self.get_list_product_dict(response)
        return product

    def parse_product(self, response: Response) -> Iterator[Dict]:
        product = self.check_product(response)
        if product:
            return WineItem(**product)
        return

    def parse_list_product(self, r: Response, s) -> Iterator[Dict]:
        product = self.check_list_product(r, s)
        if product:
            return WineItem(**product)
        return
