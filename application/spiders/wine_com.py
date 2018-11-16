import re

from typing import Iterator, Dict, IO

from scrapy import FormRequest
from scrapy.http.request import Request
from scrapy.http.response import Response
from scrapy.crawler import CrawlerProcess

from application.scrapers.spider_scraper import get_spider_settings
from application.spiders.base.abstracts.spider import AbstractSpider
from application.spiders.base.abstracts.product import AbstractParsedProduct
from application.spiders.base.wine_item import WineItem

BASE_URL = 'https://www.wine.com'


def clean(s):
    return s.replace('\r', '').replace('\n', '').strip()


class ParsedProduct(AbstractParsedProduct):

    def __init__(self, r: Response) -> None:
        super().__init__(r)
        self.result['msrp'] = self.get_msrp()

    def get_name(self) -> str:
        return self.r.xpath(
            '//h1[@class="pipName"]/text()').extract_first()

    def get_description(self) -> str:
        description = self.r.xpath(
            '//div[@class="pipWineNotes"]/div/div[1]//text()[not(ancestor::em)]'
        ).extract()
        description = ' '.join([clean(desc) for desc in description])
        return description

    def get_sku(self) -> str:
        return self.r.xpath(
            '//div[@class="prodItemStock_addCart"]/@data-sku'
        ).extract_first()

    def get_wine_type(self):
        return self.r.meta['wine_type']

    def get_msrp(self) -> float:
        msrp = self.result['price']
        msrp_selector = self.r.xpath(
            '//span[@class="productPrice_price-regWhole"][1]/text()')
        if msrp_selector:
            msrp = msrp_selector.extract_first()
            msrp = msrp.replace(',', '')
            fractional = self.r.xpath(
                '//span[@class="productPrice_price-regFractional"][1]/text()')
            if fractional:
                msrp = '.'.join([msrp, fractional.extract_first()])
            msrp = float(msrp)
        return msrp

    def get_vintage(self) -> str:
        vintage = None
        match = re.match(r'.*([1-3][0-9]{3})', self.name)
        if match:
            vintage = match.group(1)
        return vintage

    def get_price(self) -> float:
        price = self.r.xpath(
            '//span[@class="productPrice_price-saleWhole"][1]/text()'
        ).extract_first()
        fractional = self.r.xpath(
            '//span[@class="productPrice_price-saleFractional"][1]/text()'
        ).extract_first()
        price = price.replace(',', '')
        if fractional:
            price = '.'.join([price, fractional])
        try:
            float_price = float(price)
        except ValueError:
            return "ERROR READING PRICE"
        else:
            return float_price

    def get_image(self) -> str:
        image_link = self.r.xpath(
            '//span[@class="pipThumb pipThumb-1"]/img/@src').extract_first()
        if not image_link:
            image_link = self.r.xpath(
                '//img[@class="pipHero_image-default"]/@src').extract_first()
            if image_link:
                image_link = image_link.replace(
                    'w_767,c_fit,q_auto:good,fl_progressive',
                    'w_1080')
                image_link = image_link.replace(
                    'h_50,c_fit,fl_progressive',
                    'w_1080')
        return '/'.join([BASE_URL, image_link])

    def get_alcohol_pct(self) -> str:
        return self.r.xpath(
            '//span[@class="prodAlcoholPercent_percent"]/text()'
        ).extract_first()

    def get_varietals(self) -> list:
        return self.r.xpath(
            '//span[@class="prodItemInfo_varietal"]/text()'
        ).extract()

    def get_region(self) -> str:
        return self.r.xpath(
            '//h2[@class="productPageContentHead_title"][1]/text()'
        ).extract_first()

    def get_additional(self):
        additional = {
            'bottle_size': 0,
        }
        return additional

    def get_bottle_size(self) -> int:
        bottle_size = 750
        if 'half-bottle' in self.name:
            bottle_size = 375
        elif '500 ML' in self.name:
            bottle_size = 500
        elif '1 Liter' in self.name:
            bottle_size = 1000
        elif '1.5 Liter' in self.name:
            bottle_size = 1500
        return bottle_size

    def get_reviews(self) -> list:
        reviews = []
        review_rows = self.r.xpath(
            '//div[@class="pipProfessionalReviews_list"]')
        for row in review_rows:
            score_str = row.xpath(
                'div/span[@class="wineRatings_rating"]/text()'
            ).extract_first()
            reviewer_name = row.xpath(
                'div[@class="pipProfessionalReviews_authorName"]/text()'
            ).extract_first()
            content = row.xpath(
                'div/div[@class="pipSecContent_copy"]/text()'
            ).extract_first()
            if content:
                content = clean(content)
            reviews.append({'reviewer_name': reviewer_name,
                            'score_num': score_str and int(score_str) or None,
                            'score_str': score_str,
                            'content': content,
                            })
        return reviews

    def get_qoh(self) -> int:
        qoh = self.r.xpath(
            '//select[@class="prodItemStock_quantitySelect"]/option/@value')
        try:
            qoh = int(qoh.pop().extract())
        except IndexError:
            qoh = 0
        return qoh


class WineComSpider(AbstractSpider):
    """'Spider' which is getting data from wine.com"""

    name = 'wine_com'
    LOGIN = "wine_shoper@protonmail.com"
    PASSWORD = "ilovewine1B"

    def start_requests(self) -> Iterator[Dict]:
        yield Request(
            f'{BASE_URL}/auth/signin',
            callback=self.login
        )

    def before_login(self, response: Response):
        pass

    def login(self, response: Response) -> Iterator[Dict]:
        # from scrapy.shell import inspect_response
        # inspect_response(response, self)
        csrf_token = response.xpath(
            '//meta[@name="csrf"]/@content')
        token = csrf_token.extract_first()
        headers = {
                    # 'x-requested-with': 'XMLHttpRequest',
                   'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
                   # 'content-type': 'application/json',
                   # 'content-length': '122',
                   # 'accept': 'application/json, text/javascript, */*; q=0.01',
                   # 'accept-encoding': 'gzip, deflate, br',
                   # 'accept-language': 'en-US,en;q=0.9',
                   # 'origin': 'https://www.wine.com',
                   # 'referer': 'https://www.wine.com/auth/signin'
        }
        yield FormRequest(
            f'{BASE_URL}/api/userProfile/credentials/?csrf={token}',
            formdata={'login': self.LOGIN,
                      'password': self.PASSWORD,
                      'email': '',
                      'hashCode': '',
                      'prospectId': '0',
                      'rememberMe': 'false',
                      },
            headers=headers,
            meta={'csrf': token},
            callback=self.parse_wine_types
        )

    def is_not_logged(self, response):
        return "Welcome" not in response.body.decode('utf-8')  # TODO Check if John is logged

    def get_wine_types(self, response: Response) -> list:
        res = []
        varietal_div = response.xpath(
            "//div[contains(concat(' ', @class, ' '), ' varietal ')]")
        rows = varietal_div.xpath('div/ul[@class="filterMenu_list"]/li')
        for row in rows:
            wine_filter = row.xpath(
                'a[@class="filterMenu_itemLink"]/@href').extract_first()
            wine_type = row.xpath(
                'a/span[@class="filterMenu_itemName"]/text()'
            ).extract_first()
            wine_type = wine_type.replace('Champagne & ', '')
            wine_type = wine_type.replace('Rosé', 'Rose')
            wine_type = wine_type.replace(', Sherry & Port', '')
            wines_total = row.xpath(
                'a/span[@class="filterMenu_itemCount"]/text()'
            ).extract_first()
            wines_total = int(wines_total)
            if 'Saké' in wine_type:
                continue
            res.append((wine_type, wines_total, wine_filter))
        return res

    def get_listpages(self, response: Response) -> Iterator[Dict]:
        wine_types = self.get_wine_types(response)
        step = 25
        for (wine_type, wines_total, wine_filter) in wine_types:
            items_scraped = 0
            url = wine_filter
            if wines_total % step or wines_total < step:
                wines_total += step
            total_pages = int(wines_total / 25)
            for page_num in range(1, total_pages + 1):
                if items_scraped <= wines_total:
                    yield Request(
                        f'{BASE_URL}{url}/{page_num}',
                        callback=self.parse_listpage,
                        meta={'wine_type': wine_type},
                    )
                items_scraped += step

    def parse_wine_types(self, response: Response) -> Iterator[Dict]:
        if self.is_not_logged(response):
            yield
        else:
            wines_url = f'{BASE_URL}/list/wine/7155'
            yield Request(wines_url,
                          callback=self.get_listpages)

    def parse_listpage(self, response: Response) -> Iterator[Dict]:
        """Process http response
        :param response: response from ScraPy
        :return: iterator for data
        """
        # from scrapy.utils.response import open_in_browser
        # open_in_browser(response)
        selector = '//a[@class="prodItemInfo_link"]/@href'
        rows = response.xpath(selector)
        product_links = rows.getall()
        for product_link in product_links:
            absolute_url = BASE_URL + product_link
            yield Request(
                absolute_url,
                callback=self.parse_product,
                meta={'wine_type': response.meta['wine_type']},
                priority=1)

    def parse_product(self, response: Response) -> Iterator[Dict]:
        return WineItem(**ParsedProduct(response).as_dict())


def get_data(tmp_file: IO) -> None:
    settings = get_spider_settings(tmp_file)
    process = CrawlerProcess(settings)
    process.crawl(WineComSpider)
    process.start()


if __name__ == '__main__':
    import os
    current_path = os.getcwd()
    file_name = os.path.join(current_path, 'wine_com.txt')
    with open(file_name, 'w') as out_file:
        get_data(out_file)
