from typing import Iterator, Dict, IO, List
import re
import requests


from scrapy import Selector
from scrapy.http.request import Request
from scrapy.exceptions import CloseSpider
from scrapy.http.response import Response
from scrapy.crawler import CrawlerProcess

from application.logging import logger
from application.scrapers.spider_scraper import get_spider_settings
from application.spiders.base.abstracts.spider import AbstractSpider
from application.spiders.base.abstracts.pipeline import (
    BaseFilterPipeline,
    BaseIncPipeline
)
from application.spiders.base.abstracts.product import (
    AbstractParsedProduct,
    AbstractListProduct
)

BASE_URL = 'https://www.totalwine.com'
REVIEWS_URL = (
    'https://api.bazaarvoice.com/data/batch.json?'
    'passkey=caXf20wfT6xLqTTQsMoGuj0lZGIdLvCKtusCWAIn0zU9E&'
    'apiversion=5.5&&resource.q0=reviews&filter.q0=isratingsonly:eq:false'
    '&filter.q0=productid:eq:{0}&filter.q0=contentlocale:eq:en_US&'
    'stats.q0=reviews&filteredstats.q0=reviews&include.q0=authors,products,'
    'comments&filter_reviews.q0=contentlocale:eq:en_US&filter_reviewcomments'
    '.q0=contentlocale:eq:en_US&filter_comments.q0=contentlocale:eq:en_US')


class ParsedListPageProduct(AbstractListProduct):

    def get_name(self) -> str:
        name = self.s.xpath(
            'div//h2/a/text()'
        ).extract_first()
        return self.clean(name or '')

    def get_price(self) -> float:
        price = self.s.xpath(
            'div//div[@class="plp-product-buy-actual-price"]/span/text()'
        ).extract_first()
        if not price:
            price = self.s.xpath(
                'div//span[@class="price"]/text()'
            ).extract_first()
            if price:
                price = re.search(r'[0-9\.]+', price).group()
        try:
            float_price = float(price)
        except ValueError:
            return price
        else:
            return float_price

    def get_qoh(self):
        na = self.s.xpath(
            'div//div[@class="plp-product-buy-limited"]/text()'
        ).extract_first()
        if na and 'Not currently available' in na:
            qoh = 0
        else:
            qoh = 100
        return qoh

    def get_url(self):
        url = self.s.xpath(
            'div//h2/a/@href'
        ).extract_first()
        return url


class ParsedProduct(AbstractParsedProduct):

    def __init__(self, r: Response) -> None:
        super().__init__(r)
        self.result['msrp'] = self.get_msrp()
        self.result['characteristics'] = self.get_characteristics()
        self.result['description'] = self.get_description()

    def get_name(self) -> str:
        return self.r.xpath(
            '//meta[@itemprop="name"]/@content'
        ).extract_first()

    def get_characteristics(self) -> str:
        return self.additional['characteristics']

    def get_description(self) -> str:
        return self.r.xpath(
            "//div[contains(@class,'detailsTabReview')]/div/text()"
        ).extract_first()

    def get_sku(self) -> str:
        return self.r.xpath(
            '//div[text()="SKU"]/following-sibling::div[1]/text()'
        ).extract_first()

    def get_wine_type(self):
        return self.r.meta['wine_type']

    def get_msrp(self) -> float:
        msrp = self.result['price']
        msrp_selector = self.r.xpath('//div[@id="edlpPrice"]/text()')
        if msrp_selector:
            msrp = msrp_selector.extract_first()
            msrp = msrp.replace('$', '')
            msrp = msrp.replace(',', '')
            msrp = float(msrp)
        return msrp

    def get_vintage(self) -> str:
        res = ''
        match = re.match(r'.*([1-3][0-9]{3})', self.name)
        if match:
            res = match.group(1)
        return res

    def get_price(self) -> float:
        price = self.r.xpath(
            '//div[@id="ltsPrice"]/text()'
        ).extract_first()
        if not price:
            price = self.r.xpath(
                '//div[@id="edlpPrice"]/text()'
            ).extract_first()
        price = price and price.replace('$', '').replace(',', '') or 0
        try:
            float_price = float(price)
        except ValueError:
            return "ERROR READING PRICE"
        else:
            return float_price

    def get_image(self) -> str:
        return self.r.xpath(
            "//div[contains(@class,'prodImage')]/picture/img/@src"
        ).extract_first()

    def get_additional(self):

        varietal = self.r.xpath(
            '//div[text()="VARIETAL"]/following-sibling::div[1]/text()'
        ).extract_first() or ''
        region = self.r.xpath(
            '//div[text()="REGION"]/following-sibling::div/a/text()'
        ).extract_first() or ''
        style = self.r.xpath(
            '//div[text()="STYLE"]/following-sibling::div[1]/text()'
        ).extract_first() or ''
        taste = self.r.xpath(
            '//div[text()="TASTE"]/following-sibling::div[1]/text()'
        ).extract_first() or ''
        body = self.r.xpath(
            '//div[text()="BODY"]/following-sibling::div[1]/text()'
        ).extract_first() or ''
        chars = filter(bool, [style, taste, body])
        characteristics = self.clean(', '.join(chars))
        additional = {
            'varietals': [self.clean(varietal)],
            'region': self.clean(region),
            'characteristics': characteristics,
        }
        return additional

    def get_reviews(self) -> list:
        """Reviews for totalwine.com are loaded with XHR request from the
        https://api.bazaarvoice.com. We made similar request and put it's
        result into `request.meta['reviews_json']`"""
        reviews = []
        reviews_json = self.r.meta['reviews_json']
        review_results = reviews_json['BatchedResults']['q0']['Results']
        for review_result in review_results:
            score = review_result['Rating']
            score *= 20
            score_str = str(score)
            reviewer_name = review_result['UserNickname']
            content = (review_result['ReviewText'] or
                       review_result['Title'] or '')
            content = self.clean(content)
            reviews.append({'reviewer_name': reviewer_name,
                            'score_num': score,
                            'score_str': score_str,
                            'content': content,
                            })
        return reviews

    def get_qoh(self) -> int:
        unavailable = self.r.xpath(
            '//button[@atc="product_Add to cart"]/text()'
        ).extract_first()
        if unavailable in ('Unavailable',
                           'Out of Stock'):
            return 0
        single = self.r.xpath(
            '//*[contains(@class, "packageDescription_")]'
        ).extract_first()
        if single and 'Single' not in single:
            # Detect multipacks which will be ignored by the FilterPipeline
            return 0
        return 100


class TotalWineSpider(AbstractSpider):
    """'Spider' which is getting data from totalwine.com"""

    name = 'totalwine'
    LOGIN = "wine_shoper@protonmail.com"
    PASSWORD = "ilovewine1B"
    filter_pipeline = "application.spiders.totalwine.FilterPipeline"
    inc_filter_pipeline = "application.spiders.totalwine.IncFilterPipeline"

    def start_requests(self) -> Iterator[Dict]:
        yield Request('http://checkip.dyndns.org/', callback=self.check_ip)
        yield Request(
            f'{BASE_URL}/login',
            callback=self.login
        )

    def check_ip(self, response):
        pub_ip = response.xpath(
            '//body/text()').re(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}'
        )[0]
        logger.info('MY IP IS %s', pub_ip)

    def before_login(self):
        pass

    def login(self, response: Response) -> Iterator[Dict]:
        # from scrapy.shell import inspect_response
        # inspect_response(response, self)
        csrf_token = response.xpath(
            '//input[@id="CSRFToken"]/@value'
        ).extract_first()
        cookie = (
            '"agePresent|Sacramento (Arden), CA|/events/dec-2018/'
            'california/sacramento-arden|Anonymous'
            f'|0|geoLocationUnidentified||{csrf_token}|"')
        cookies = {'cacheCookie': cookie,
                   'age': 'present',
                   }
        headers = {
           'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
        }
        yield Request(
            f'{BASE_URL}/wine/c/c0020?volume=standard-750-ml&storeclear=true&viewall=true',
            headers=headers,
            cookies=cookies,
            callback=self.get_listpages
        )

    def get_wine_types(self, response: Response) -> list:
        res = []
        rows = response.xpath(
            '//li[3]/div/ul/li'
        )
        for row in rows:
            wine = row.xpath(
                'a/span[@class="checkStyle"]/label/text()'
            ).extract_first()
            if not wine:
                continue
            wine_type = ' '.join(wine.split()[:-1])
            wine_type = wine_type.replace(' Wine', '')
            if 'Dessert' in wine_type:
                wine_type = 'Dessert'
            elif 'Sparkling' in wine_type:
                wine_type = 'Sparkling'
            elif 'Rose' in wine_type:
                wine_type = 'Rose'
            allowed_wines = ('Red', 'Rose', 'White', 'Sparkling', 'Dessert')
            if wine_type not in allowed_wines:
                continue

            wines_total = wine.split()[-1][1:-1]
            wines_total = int(wines_total)
            wine_filter = row.xpath(
                'a/@data-href').extract_first()
            wine_filter = f'{wine_filter}&pagesize=180'
            res.append((wine_type, wines_total, wine_filter))
        if not res:
            raise CloseSpider('Wine types not found!')
        return res

    def is_not_logged(self, response):
        pass

    def parse_wine_types(self, response: Response) -> Iterator[Dict]:
        pass

    def get_listpages(self, response: Response) -> Iterator[Dict]:
        """Process http response
        :param response: response from ScraPy
        :return: iterator for data
        """
        wine_types = self.get_wine_types(response)
        step = 180
        for (wine_type, wines_total, wine_filter) in wine_types:
            items_scraped = 0
            url = wine_filter
            if wines_total % step or wines_total < step:
                wines_total += step
            total_pages = int(wines_total / step)
            for page_num in range(1, total_pages + 1):
                if items_scraped <= wines_total:
                    yield Request(
                        f'{url}&page={page_num}',
                        callback=self.parse_listpage,
                        meta={'wine_type': wine_type},
                    )
                items_scraped += step

    def parse_listpage(self, response: Response) -> Iterator[Dict]:
        """Process http response
        :param response: response from ScraPy
        :return: iterator for data
        """
        if self.is_not_logged(response):
            self.logger.exception("Login failed")
            yield
        else:
            full_scrape = self.settings['FULL_SCRAPE']
            rows = response.xpath(
                '//ul[@class="plp-list"]/li')
            links = []
            for row in rows:
                if full_scrape:
                    na = row.xpath(
                        'div/div/div/div/div/div[@class'
                        '="plp-product-buy-limited"]/text()'
                    ).extract_first()
                    na = AbstractParsedProduct.clean(na)
                    if na:
                        continue

                    link = row.xpath(
                        'div/div/div/a/@href'
                    ).extract_first()
                    if link:
                        links.append(link)
                    else:
                        logger.exception(
                            'Link not fount for %s '
                            'on page: %s', row, response.url
                        )
                else:
                    yield self.parse_list_product(row)
            for link in links:
                absolute_url = BASE_URL + link
                product_id = re.findall(r'\d+', link)[0]
                reviews_url = REVIEWS_URL.format(product_id)
                reviews_json = requests.get(reviews_url).json()
                yield Request(
                    absolute_url,
                    callback=self.parse_product,
                    meta={'wine_type': response.meta.get('wine_type'),
                          'reviews_json': reviews_json},
                    priority=1)

    @property
    def ignored_images(self) -> List[str]:
        return ['8962178744350.png']

    def get_product_dict(self, response: Response):
        return ParsedProduct(response).as_dict()

    def get_list_product_dict(self, s: Selector):
        return ParsedListPageProduct(s).as_dict()


class FilterPipeline(BaseFilterPipeline):

    IGNORED_IMAGES = ['8962178744350.png']

    def _check_multipack(self, item: dict):
        # Multipacks are detected on the scrape stage (set qoh=0)
        pass


class IncFilterPipeline(BaseIncPipeline):

    pass


def get_data(tmp_file: IO) -> None:
    settings = get_spider_settings(tmp_file, TotalWineSpider, full_scrape=False)
    process = CrawlerProcess(settings)
    process.crawl(TotalWineSpider)
    process.start()


if __name__ == '__main__':
    import os
    from application import create_app
    app = create_app()
    with app.app_context():
        current_path = os.getcwd()
        file_name = os.path.join(current_path, 'totalwine.txt')
        with open(file_name, 'w') as out_file:
            get_data(out_file)
