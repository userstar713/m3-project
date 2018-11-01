from abc import ABC, abstractmethod
from typing import Dict
from scrapy.http.response import Response


class AbstractParsedProduct(ABC):

    def __init__(self, r: Response) -> None:
        self.r = r
        self.name = self.get_name()
        self.additional = self.get_additional()
        self.result = {
            'url': self.get_url(),
            'name': self.name,
            'vintage': self.get_vintage(),
            'description': self.get_description(),
            'price': self.get_price(),
            'image': self.get_image(),
            'qoh': self.get_qoh(),
            'varietals': self.get_varietals(),
            'region': self.get_region(),
            'alcohol_pct': self.get_alcohol_pct(),
            'wine_type': self.get_wine_type(),
            'reviews': self.get_reviews(),
            'bottle_size': self.get_bottle_size(),
            'sku': self.get_sku(),
        }

    def get_url(self):
        return self.r.url

    def get_wine_type(self):
        return r.meta.get('wine_type')

    @abstractmethod
    def get_sku(self):
        pass

    @abstractmethod
    def get_name(self):
        pass

    @abstractmethod
    def get_vintage(self):
        pass

    @abstractmethod
    def get_price(self):
        pass

    @abstractmethod
    def get_image(self):
        pass

    def get_varietals(self) -> list:
        return self.additional['varietals']

    def get_region(self) -> str:
        return self.additional['region']

    def get_alcohol_pct(self) -> str:
        return self.additional['alcohol_pct']

    def get_description(self) -> str:
        return self.additional['description']

    @abstractmethod
    def get_additional(self):
        pass

    @abstractmethod
    def get_bottle_size(self):
        pass

    @abstractmethod
    def get_reviews(self):
        pass

    @abstractmethod
    def get_qoh(self):
        pass

    def as_dict(self) -> Dict:
        return self.result
