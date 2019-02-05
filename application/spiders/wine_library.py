import re

from typing import Iterator, Dict, IO, List

from scrapy import (
    FormRequest,
    Selector
)
from scrapy.http.request import Request
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

BASE_URL = 'https://winelibrary.com'
# BASE FILTER includes filters: Library Pass Items, 750 ML, Non-Presale
# Red, White, Rose wine types
BASE_FILTER = ('/search?search=&lpass=1&color[]=Red&color[]=White&color[]='
               'Rose&size[]=750ML&presale[]=Non-Presale')


def get_qoh(response):
    row = response.xpath(
        '//div[@id="qty-box"]/input')
    qty = row.xpath('@data-running-out-stop').extract_first()
    max_qty = row.xpath('@max').extract_first()
    return qty and int(qty) or max_qty and int(max_qty)


class ParsedListPageProduct(AbstractListProduct):

    def get_name(self) -> str:
        name = self.s.xpath(
            'div/div/h5/a/span[@class="js-elip-multi"]/text()'
        ).extract_first()
        return self.clean(name or '')

    def get_price(self) -> float:
        s = self.s.xpath(
            'div/div/h4[@class="search-item-price"]/text()'
        ).extract_first()
        if not s:
            return 0
        s = self.clean(s)
        s = s.replace('$', '').replace(',', '')
        try:
            float_s = float(s)
        except ValueError:
            return "ERROR READING PRICE"
        else:
            return float_s

    def get_qoh(self):
        pass

    def get_url(self):
        relative_url = self.s.xpath(
            'div/div/h5/a/@href'
        ).extract_first()
        return f'{BASE_URL}{relative_url}'


class ParsedProduct(AbstractParsedProduct):

    def __init__(self, r: Response) -> None:
        super().__init__(r)
        self.result['msrp'] = self.get_msrp()
        self.result['characteristics'] = self.get_characteristics()
        self.result['description'] = self.get_description()

    def get_name(self) -> str:
        return self.r.xpath(
            '//h1[@class="h2 h-sans product-pg-title"]/text()'
        )[0].extract()

    def get_characteristics(self) -> str:
        return self.additional['characteristics']

    def get_description(self) -> str:
        desc = self.r.xpath(
            "//p[@itemprop='description']/text()"
        ).extract_first()
        desc = desc and self.clean(desc) if desc else ''
        if desc.startswith('"') and desc.endswith('"'):
            desc = desc[1:-1]
        features = self.r.xpath(
            '//td[text()="Features"]/following-sibling::td[1]/text()'
        ).extract_first()
        if features:
            desc = f'{desc} {features}'
        closure = self.r.xpath(
            '//td[text()="Closure"]/following-sibling::td[1]/text()'
        ).extract_first()
        if closure:
            desc = f'{desc} {closure}'
        return desc

    def get_sku(self) -> str:
        return self.additional['sku']

    def get_wine_type(self):
        return self.additional['wine_type']

    def get_msrp(self) -> float:
        msrp = self.result['price']
        msrp_selector = self.r.xpath('//span/span[@class="strike"]/text()')
        if msrp_selector:
            msrp = msrp_selector[0].extract()
            msrp = msrp.replace('$', '')
            msrp = msrp.replace(',', '')
            msrp = float(msrp)
        return msrp

    def get_vintage(self) -> str:
        vintage = self.additional['vintage']
        if not vintage:
            match = re.match(r'.*([1-3][0-9]{3})', self.name)
            if match:
                vintage = match.group(1)
        return vintage

    def get_price(self) -> float:
        price = self.r.xpath(
            '//span[@itemprop="price"]/text()'
        )[0].extract()
        price = price.replace('$', '').replace(',', '')
        try:
            float_price = float(price)
        except ValueError:
            return "ERROR READING PRICE"
        else:
            return float_price

    def get_image(self) -> str:
        return self.r.xpath(
            '//div[@class="product-pg-photo"]/a/'
            'img[@class="img-full-responsive"]/@src')[0].extract()

    def get_additional(self):
        additional = {
            'varietals': [],
            'vintage': None,
            'name_varietal': None,
            'region': None,
            'other': None,
            'sku': None,
            'wine_type': None,
        }
        characteristics = []
        rows = self.r.xpath('//div[@class="product-detail-tables row"]//tr')
        for row in rows:
            detail_name = row.xpath(
                'td[@class="label"]/text()').extract()[0]
            value_selector = row.xpath(
                'td[@class="data"]')
            detail_value = row.xpath(
                'td[@class="data"]/text()').extract()
            detail_value = detail_value and detail_value[0]
            a_value = value_selector.xpath('a/text()').extract()
            if detail_value == 'N/A':
                continue

            if '#' in detail_name:
                additional['sku'] = detail_value
            elif detail_name in ('Country',
                                 'Region',
                                 'Sub-Region'):
                detail_value = a_value
                additional['region'] = detail_value and detail_value[0]
            elif detail_name == 'Ratings':
                pass
            elif detail_name == 'Vintage':
                detail_value = a_value
                if detail_value:
                    additional['vintage'] = detail_value[0]
            elif detail_name == 'Color':
                detail_value = a_value
                if detail_value and detail_value[0] in ('White', 'Red', 'Rose'):
                    additional['wine_type'] = detail_value[0]
            elif detail_name == 'ABV':
                additional['alcohol_pct'] = detail_value.replace('%', '')
            elif detail_name == 'Varietal(s)':
                additional['varietals'] = value_selector.xpath(
                    'a/text()').extract()
            elif detail_name == 'Size':
                detail_value = detail_value.replace('mL', '')
                detail_value = detail_value.replace('l', '')
                if '.' in detail_value:
                    detail_value = float(detail_value) * 100
                else:
                    try:
                        detail_value = int(detail_value)
                    except ValueError:
                        logger.exception(
                            f'Invalid int() value: {detail_value}')
                        detail_value = ''
                if detail_value and detail_value < 10:
                    detail_value *= 1000
                additional['bottle_size'] = detail_value
            elif detail_name == 'Closure':
                pass
            elif detail_name == 'Features':
                if detail_value in ('Dessert', 'Sparkling'):
                    additional['wine_type'] = detail_value
            elif detail_name in ('Taste', 'Nose'):
                if detail_value:
                    characteristic = self.clean(detail_value)
                    characteristics.append(characteristic)
        additional['characteristics'] = ', '.join(characteristics)
        return additional

    def get_bottle_size(self) -> int:
        return self.additional['bottle_size']

    def get_reviews(self) -> list:
        reviews = []
        review_rows = self.r.xpath('//div[@itemprop="review"]/p')
        for i, row in enumerate(review_rows):
            score = row.xpath('b/span[@itemprop="reviewRating"]')
            score_str = ''
            reviewer_name = ''
            if score:
                score_str = score.xpath(
                    'span[@itemprop="ratingValue"]/text()').extract()[0]
                reviewer_name = row.xpath(
                    '//span[@itemprop="name"]/text()').extract()[0]
            else:
                score = row.xpath('b/text()').extract()
                if score:
                    score = self.clean(score[0])
                    scoring = score.split(' ')
                    score_str = scoring[0].isdigit() and scoring[0]
                    reviewer_name = score.replace(f'{score_str} ', '')
            if not score_str:
                continue
            if reviewer_name:
                reviewer_name = self.match_reviewer_name(reviewer_name)

            content = review_rows[i + 1].xpath('text()').extract() or ['']
            if content:
                content = self.clean(content[0])
                if content.startswith('"') and content.endswith('"'):
                    content = content[1:-1]
            if score_str:
                score_str = ''.join(score_str.split('-')[-1])
                score_str = score_str.replace('+', '')
            reviews.append({'reviewer_name': reviewer_name,
                            'score_num': score_str and int(score_str) or None,
                            'score_str': score_str,
                            'content': content,
                            })
        return reviews

    def get_qoh(self) -> int:
        qoh = get_qoh(self.r)
        return qoh


class WineLibrarySpider(AbstractSpider):
    """'Spider' which is getting data from winelibrary.com"""

    name = 'wine_library'
    LOGIN = "wine_shoper@protonmail.com"
    PASSWORD = "ilovewine1B"
    filter_pipeline = "application.spiders.wine_library.FilterPipeline"
    inc_filter_pipeline = "application.spiders.wine_library.IncFilterPipeline"

    def start_requests(self) -> Iterator[Dict]:
        yield Request(
            f'{BASE_URL}/sign_in',
            callback=self.login
        )

    def before_login(self, response):
        pass

    def login(self, response: Response) -> Iterator[Dict]:
        token_path = response.xpath(
            '//form[@id="sessionform"]'
            '//input[@name="authenticity_token"]/@value')
        token = token_path[0].extract()
        return FormRequest.from_response(
            response,
            formxpath='//form[@id="sessionform"]',
            formdata={'user_wine_library_detail[email]': self.LOGIN,
                      'user_wine_library_detail[password]': self.PASSWORD,
                      'user_wine_library_detail[remember_me]': '0',
                      'authenticity_token': token,
                      },
            callback=self.parse_wine_types
        )

    def is_not_logged(self, response):
        return "My Account" not in response.body.decode('utf-8')

    def get_listpages(self, response: Response) -> Iterator[Dict]:
        total_pages_link = response.xpath(
            '//li[@class="page-num last"]/a/@href').extract()[0]
        total = re.findall(r'page=\d*', total_pages_link)[0]
        total_pages = int(total.replace('page=', ''))
        for page_num in range(1, total_pages + 1):
            yield Request(
                f'{BASE_URL}{BASE_FILTER}&page={page_num}',
                callback=self.parse_listpage,
            )

    def parse_wine_types(self, response: Response) -> Iterator[Dict]:
        if self.is_not_logged(response):
            yield
        else:
            wines_url = f'{BASE_URL}{BASE_FILTER}'
            yield Request(wines_url,
                          callback=self.get_listpages)

    def parse_listpage(self, response: Response) -> Iterator[Dict]:
        """Process http response
        :param response: response from ScraPy
        :return: iterator for data
        """
        if self.is_not_logged(response):
            logger.exception("Login failed")
            yield
        else:
            full_scrape = self.settings['FULL_SCRAPE']
            res = response.xpath('//div[@id="result_pane"]')
            if full_scrape:
                links = res.xpath(
                    '//h5[@class="h-sm search-item-title"]/a/@href'
                ).extract()
                for link in links:
                    absolute_url = BASE_URL + link
                    yield Request(
                        absolute_url,
                        callback=self.parse_product,
                        priority=1)
            else:
                products = res.xpath('div[@class="search-item"]')
                for product in products:
                    yield self.parse_list_product(product)

    @property
    def ignored_images(self) -> List[str]:
        return []

    def get_product_dict(self, response: Response):
        return ParsedProduct(response).as_dict()

    def get_list_product_dict(self, s: Selector):
        return ParsedListPageProduct(s).as_dict()


class FilterPipeline(BaseFilterPipeline):

    IGNORED_IMAGES = []


class IncFilterPipeline(BaseIncPipeline):

    def get_qoh(self, response):
        return get_qoh(response)

    def parse_detail_page(self, response):
        product = ParsedProduct(response)
        yield product.as_dict()


def get_data(tmp_file: IO) -> None:
    settings = get_spider_settings(tmp_file, WineLibrarySpider, full_scrape=True)
    process = CrawlerProcess(settings)
    process.crawl(WineLibrarySpider)
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
