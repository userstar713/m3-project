import re

from typing import Iterator, Dict, IO, List

from scrapy import FormRequest
from scrapy.http.request import Request
from scrapy.http.response import Response
from scrapy.crawler import CrawlerProcess

from application.logging import logger
from application.scrapers.spider_scraper import get_spider_settings
from application.spiders.base.abstracts.spider import AbstractSpider
from application.spiders.base.abstracts.product import AbstractParsedProduct

BASE_URL = 'https://www.totalwine.com'


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
        vintage = ''
        return vintage

    def get_price(self) -> float:
        price = self.r.xpath(
            '//div[@id="ltsPrice"]/text()'
        ).extract_first()
        if not price:
            price = self.r.xpath(
                '//div[@id="edlpPrice"]/text()'
            ).extract_first()
        price = price.replace('$', '').replace(',', '')
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
            'varietals': [varietal],
            'region': region,
            'characteristics': characteristics,
        }
        return additional

    def get_bottle_size(self) -> int:
        size = self.r.xpath(
            '//h2[contains(@class,"productSubTitle")]/text()'
        ).extract_first()
        size = size.replace('ml', '')
        size = int(size)
        return size

    def get_reviews(self) -> list:
        reviews = []
        review_rows = self.r.xpath('//li[@itemprop="review"]')
        for row in review_rows:
            score = row.xpath(
                'div[2]/div/div/div/div/div/span/meta[@itemprop="ratingValue"]/@content'
            ).extract_first()
            score *= 20
            score_str = str(score)
            reviewer_name = row.xpath(
                '//div[1]/div/div/div/div/button/h3/text()').extract_first()
            content = self.r.xpath(
                'div[2]/div/div/div[2]/div/div/div/p/text()'
            ).extract_first()
            content = self.clean(content)
            reviews.append({'reviewer_name': reviewer_name,
                            'score_num': score,
                            'score_str': score_str,
                            'content': content,
                            })
        return reviews

    def get_qoh(self) -> int:
        return 100


class TotalWineSpider(AbstractSpider):
    """'Spider' which is getting data from totalwine.com"""

    name = 'totalwine'
    LOGIN = "wine_shoper@protonmail.com"
    PASSWORD = "ilovewine1B"

    def start_requests(self) -> Iterator[Dict]:
        yield Request(
            f'{BASE_URL}/login',
            callback=self.login
        )

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
            'california/sacramento-arden?selectstore=true|Anonymous'
            f'|0|geoLocationUnidentified||{csrf_token}|"')
        cookies = {'cacheCookie': cookie,
                   'age': 'present'}
        headers = {
           'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
        }
        yield Request(
            f'{BASE_URL}/wine/c/c0020?volume=standard-750-ml&tab=fullcatalog&storeclear=true&viewall=true',
            headers=headers,
            cookies=cookies,
            callback=self.get_listpages
        )

    def get_wine_types(self, response: Response) -> list:
        res = []
        rows = response.xpath(
            '//li[@class="act"]'
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
                break  # TODO REMOVE ME

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
                    if row:
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
                    if row:
                        yield self.parse_list_product(response, row)
            for link in links:
                absolute_url = BASE_URL + link
                yield Request(
                    absolute_url,
                    callback=self.parse_product,
                    meta={'wine_type': response.meta.get('wine_type')},
                    priority=1)

    @property
    def ignored_images(self) -> List[str]:
        return []

    def get_product_dict(self, response: Response):
        return ParsedProduct(response).as_dict()

    def get_list_product_dict(self, response: Response):
        raise NotImplementedError

    def check_prearrival(self, product: dict, response: Response):
        text = response.xpath(
            '//div[@class="alert_message mb5"]/strong/text()'
        ).extract_first() or ''
        return self.is_prearrival(text)

    def check_multipack(self, product: dict, response: Response):
        size_label = response.xpath(
            '//td[text()="Size"]/following-sibling::td[1]/text()'
        ).extract_first()
        return size_label == 'each'


def get_data(tmp_file: IO) -> None:
    settings = get_spider_settings(tmp_file)
    process = CrawlerProcess(settings)
    process.crawl(TotalWineSpider)
    process.start()


if __name__ == '__main__':
    import os
    from application import create_app
    app = create_app()
    with app.app_context():
        current_path = os.getcwd()
        file_name = os.path.join(current_path, 'winelibrary.txt')
        with open(file_name, 'w') as out_file:
            get_data(out_file)
