import re
from abc import ABC, abstractmethod
from typing import Iterator, Dict, List
from scrapy.exceptions import DropItem
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

    def _check_bottle_size(self, bottle_size: int) -> bool:
        return bottle_size == 750

    def _check_product_image(self, image: str) -> bool:
        if not image or 'default_bottle' in image:
            return False
        relative_image = image.split('/')[-1]
        return relative_image not in self.ignored_images

    def check_product(self, product: dict, response: Response):
        # TODO move this checks to the own validation class
        if not self._check_bottle_size(product['bottle_size']):
            return None
        if not self._check_product_image(product['image']):
            return None
        if not product['qoh']:
            return None
        if self.check_prearrival(product, response):
            return None
        if self.check_multipack(product, response):
            return None
        return True

    @abstractmethod
    def check_prearrival(self, product: dict, response: Response) -> bool:
        pass

    @abstractmethod
    def check_multipack(self, product: dict, response: Response) -> bool:
        pass

    def is_prearrival(self, text: str) -> bool:
        regexp = re.compile('.*(Pre-ArrivaL|PRE-ORDER|Pre-Sale).*',
                            re.IGNORECASE)
        return bool(regexp.match(text))

    def check_list_product(self, product: dict, response: Response):
        return True

    @property
    @abstractmethod
    def ignored_images(self) -> List[str]:
        pass

    def parse_product(self, response: Response) -> Iterator[Dict]:
        product = self.get_product_dict(response)
        is_valid = self.check_product(product, response)
        if is_valid:
            return WineItem(**product)
        # raise DropItem(f'Invalid product {product}')
        return

    def parse_list_product(self, r: Response, s) -> Iterator[Dict]:
        product = self.get_list_product_dict(r)
        is_valid = self.check_list_product(product, r)
        if is_valid:
            return WineItem(**product)
        # return DropItem(f'Invalid product {product}')
        return