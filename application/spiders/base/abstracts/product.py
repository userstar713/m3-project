from abc import ABC, abstractmethod
from typing import Dict
from scrapy import Selector
from scrapy.http.response import Response
from sqlalchemy import text
from application.db_extension.models import db


class AbstractParsedProduct(ABC):

    def __init__(self, r: Response, s: Selector = None) -> None:
        self.r = r
        self.s = s
        self.name = self.get_name()
        self.additional = self.get_additional()
        self.result = {
            'single_product_url': self.get_url(),
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

    def match_reviewer_name(self, name: str) -> str:
        t = text("SELECT name "
                 "FROM domain_reviewers, jsonb_array_elements_text(aliases) "
                 "WHERE value ILIKE :name OR name ILIKE :name "
                 "OR substring(LOWER(:name), LOWER(name)) IS NOT NULL "
                 "LIMIT 1")
        res = db.session.execute(t, params={'name': name}).scalar()
        return res or name

    def clean(self, s):
        return s.replace('\r', '').replace('\n', '').strip()

    def get_url(self):
        return self.r.url

    def get_wine_type(self):
        return self.r.meta.get('wine_type')

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
        return self.additional.get('varietals', [])

    def get_region(self) -> str:
        return self.additional.get('region')

    def get_alcohol_pct(self) -> str:
        return self.additional.get('alcohol_pct', 0)

    def get_description(self) -> str:
        return self.additional.get('description', '')

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
