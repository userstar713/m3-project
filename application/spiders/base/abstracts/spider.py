from abc import ABC, abstractmethod
from scrapy.spiders import Spider


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

    @abstractmethod
    def get_wine_types(self, response):
        pass

    @abstractmethod
    def get_listpages(self, response):
        pass

    @abstractmethod
    def parse_wine_types(self, response):
        pass

    @abstractmethod
    def parse_listpage(self, response):
        pass

    @abstractmethod
    def parse_product(self, response):
        pass
