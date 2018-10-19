import logging

from typing import NamedTuple, Iterator, Dict, IO, Union
from itertools import count

from scrapy.item import Item
from scrapy import Field, FormRequest
from scrapy.http.request import Request
from scrapy.http.response import Response
from scrapy.selector import Selector
from scrapy.crawler import CrawlerProcess
from scrapy.exceptions import CloseSpider

from .base import BaseSpider
from OpenSSL import SSL
from scrapy.core.downloader.contextfactory import BrowserLikeContextFactory
from application.config import SCRAPER_PRODUCTS_LIMIT

class CustomCipherContextFactory(BrowserLikeContextFactory):
    """A more protocol-flexible TLS/SSL context factory.

    A TLS/SSL connection established with [SSLv23_METHOD] may understand
    the SSLv3, TLSv1, TLSv1.1 and TLSv1.2 protocols.
    See https://www.openssl.org/docs/manmaster/ssl/SSL_CTX_new.html
    """

    def __init__(self, method=SSL.TLSv1_2_METHOD, *args, **kwargs):
        super().__init__(method=method, *args, **kwargs)


    def getContext(self, hostname=None, port=None):
        ctx = super().getContext(hostname, port)
        #ctx.get_cipher_list()
        ctx.set_cipher_list('ECDHE-RSA-AES256-GCM-SHA384')
        #ctx.set_cipher_list(':'.join([
        #    'ECDHE-RSA-AES256-GCM-SHA384',
        #    'ECDHE_ECDSA_WITH_AES_256_GCM_SHA384',
        #    'ECDHE_RSA_WITH_AES_256_CBC_SHA384',
        #    'ECDHE_ECDSA_WITH_AES_256_CBC_SHA384',
        #    'ECDHE_RSA_WITH_AES_256_CBC_SHA',
        #    'ECDHE_ECDSA_WITH_AES_256_CBC_SHA',
        #    'DHE_RSA_WITH_AES_256_GCM_SHA384',
        #    'DHE_RSA_WITH_AES_256_CBC_SHA256',
        #    'DHE_RSA_WITH_AES_256_CBC_SHA',
        #    'ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256',
        #    'ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256',
        #    'DHE_RSA_WITH_CHACHA20_POLY1305',
        #    'GOST2012256 - GOST89 - GOST89',
        #    'GOST2001 - GOST89 - GOST89',
        #    'DHE_RSA_WITH_CAMELLIA_256_CBC_SHA256',
        #    'DHE_RSA_WITH_CAMELLIA_256_CBC_SHA',
        #    'RSA_WITH_AES_256_GCM_SHA384',
        #    'RSA_WITH_AES_256_CBC_SHA256',
        #    'RSA_WITH_AES_256_CBC_SHA',
        #    'RSA_WITH_CAMELLIA_256_CBC_SHA256',
        #    'RSA_WITH_CAMELLIA_256_CBC_SHA',
        #    'ECDHE_RSA_WITH_AES_128_GCM_SHA256',
        #    'ECDHE_ECDSA_WITH_AES_128_GCM_SHA256',
        #    'ECDHE_RSA_WITH_AES_128_CBC_SHA256',
        #    'ECDHE_ECDSA_WITH_AES_128_CBC_SHA256',
        #    'ECDHE_RSA_WITH_AES_128_CBC_SHA',
        #    'ECDHE_ECDSA_WITH_AES_128_CBC_SHA',
        #    'DHE_RSA_WITH_AES_128_GCM_SHA256',
        #    'DHE_RSA_WITH_AES_128_CBC_SHA256',
        #    'DHE_RSA_WITH_AES_128_CBC_SHA',
        #    'DHE_RSA_WITH_CAMELLIA_128_CBC_SHA256',
        #    'DHE_RSA_WITH_CAMELLIA_128_CBC_SHA',
        #    'RSA_WITH_AES_128_GCM_SHA256',
        #    'RSA_WITH_AES_128_CBC_SHA256',
        #    'RSA_WITH_AES_128_CBC_SHA',
        #    'RSA_WITH_CAMELLIA_128_CBC_SHA256',
        #    'RSA_WITH_CAMELLIA_128_CBC_SHA',
        #    'ECDHE_RSA_WITH_RC4_128_SHA',
        #    #'ECDHE_ECDSA_WITH_RC4_128_SHA',
        #    #'RSA_WITH_RC4_128_SHA',
        #    #'RSA_WITH_RC4_128_MD5',
        #    'ECDHE_RSA_WITH_3DES_EDE_CBC_SHA',
        #    'ECDHE_ECDSA_WITH_3DES_EDE_CBC_SHA',
        #    #'DHE_RSA_WITH_3DES_EDE_CBC_SHA',
        #    #'RSA_WITH_3DES_EDE_CBC_SHA',
        #    'DHE_RSA_WITH_DES_CBC_SHA',
        #    #'RSA_WITH_DES_CBC_SHA',
        #    'EMPTY_RENEGOTIATION_INFO_SCSV',
        #    ])
        #)
        return ctx


CONCURRENT_REQUESTS = 16
COOKIES_DEBUG = False
BASE_URL = 'https://www.klwines.com'
#DOWNLOADER_CLIENTCONTEXTFACTORY = 'scrapy.core.downloader.contextfactory.BrowserLikeContextFactory'
DOWNLOADER_CLIENTCONTEXTFACTORY = 'application.spiders.klwines.CustomCipherContextFactory'


def clean(s):
    return s.replace('\r', '').replace('\n', '').strip()


class ParsedProduct:
    def __init__(self, r: Response) -> None:
        self.r = r
        self.name = self.get_name()
        self.additional = self.get_additional()
        self.result = {
            'url': self.get_url(),
            'name': self.name,
            'price': self.get_price(),
            'image': self.get_image(),
            'qoh': self.get_qoh(),
            'varietals': self.get_varietals(),
            'region': self.get_region(),
            'alcohol_pct': self.get_alcohol_pct(),
            'wine_type': self.additional['wine_type'],
            'reviews': self.get_reviews(),
            'bottle_size': self.get_bottle_size(),
        }

    def get_url(self) -> str:
        return self.r.url

    def get_name(self) -> str:
        return clean(self.r.xpath(
            '//div[@class="result-desc"]/h1/text()'
        )[0].extract())

    def get_price(self) -> float:
        s = clean(self.r.xpath(
            f'//div[@class="result-info"]/span/span/strong/text()'
        )[0].extract())
        s = s.replace('$', '').replace(',', '')
        try:
            float_s = float(s)
        except ValueError:
            return "ERROR READING PRICE"
        else:
            return float_s

    def get_image(self) -> str:
        return BASE_URL + self.r.xpath('//img[@class="productImg"]/@src')[
            0].extract()

    def get_varietals(self) -> list:
        return self.additional['varietals']

    def get_region(self) -> str:
        return self.additional['region']

    def get_alcohol_pct(self) -> str:
        return self.additional['alcohol_pct']

    def get_additional(self):
        additional = {
            'varietals': [],
            'alcohol_pct': None,
            'name_varietal': None,
            'wine_type': None,
            'region': None,
            'other': None,
        }
        rows = self.r.xpath('div.addtl-info-block')
        for row in rows:
            title = clean(
                row.xpath('//td[@class="detail_td1"]/text()').extract())
            value = row.xpath(
                '//td[@class="detail_td"]/text()').extract()
            if title == "Alcohol Content (%):":
                additional['alcohol_pct'] = value
            elif title == "Varietal:":
                value = value.replace(" and ", " ")
                wine_type = None
                if "Other White" in value:
                    value = None
                    wine_type = ["white", "sparkling"]
                elif "Other Red" in value:
                    value = None
                    wine_type = "red"

                additional['name_varietal'] = value
                additional['varietals'] = []
                if value:
                    additional['varietals'].append(value)
                if wine_type:
                    additional['wine_type'] = wine_type
            elif title == "Country:":
                additional['country'] = value
            elif title == "Sub-Region:":
                additional['region'] = value
            elif title == "Specific Appellation:":
                additional['region'] = value
        return additional

    def get_bottle_size(self) -> int:
        bottle_size = 750
        if "187 ml" in self.name:
            bottle_size = 187
        elif "375 ml" in self.name:
            bottle_size = 375
        elif "1.5 l" in self.name:
            bottle_size = 1500
        elif "3.0 l" in self.name:
            bottle_size = 3000
        return bottle_size

    def get_reviews(self) -> list:
        reviews = []
        reviewer_point = self.r.xpath(
            '//div[@class="result-desc"]/span[@class="H2ReviewNotes"]'
        )
        texts = self.r.xpath(
            '//div[@class="result-desc"]/p'
        )
        for rp, text in zip(reviewer_point, texts):
            reviewer_name = clean(
                ''.join(rp.xpath('text()').extract())
            )
            content = clean(''.join(text.xpath('text()').extract()))

            if 'K&L' in reviewer_name:
                reviews.append({
                    'reviewer_name': 'K&LNotes',
                    'score_num': None,
                    'score_str': None,
                    'content': content
                })
            else:
                raw_points = rp.xpath('span/text()')
                if raw_points:
                    score = clean(
                        raw_points[0].extract()
                    ).replace('points', '').strip()
                    if '-' in score:
                        score = score.split('-')[-1]
                else:
                    score = None
                reviews.append({
                    'reviewer_name': reviewer_name,
                    'score_num': score and int(score),
                    'score_str': score,
                    'content': content
                })
        return reviews

    def get_qoh(self) -> int:
        rows = self.r.xpath('//div[@class=inventory]/div[@class=column]/tr')
        qoh = 0
        for row in rows:
            # location = row.xpath('/td')[0]
            qty = row.xpath('/td')[:-1]
            qoh + int(qty)
        return qoh

    def as_dict(self) -> Dict:
        return self.result


class WineItem(Item):
    name = Field()
    url = Field()
    price = Field()
    # vintage = Field()
    # msrp = Field()
    # brand = Field()
    region = Field()
    varietals = Field()
    # foods = Field()
    wine_type = Field()
    # body = Field()
    # tannin = Field()
    image = Field()
    # characteristics = Field()
    # description = Field()
    # purpose = Field()
    # sku = Field()
    bottle_size = Field()
    qoh = Field()
    # highlights = Field()
    # single_product_url = Field()
    alcohol_pct = Field()
    # drink_from = Field()
    # drink_to = Field()
    # acidity = Field()
    # discount_pct = Field()
    # flaw = Field()
    reviews = Field()

class KLWinesSpider(BaseSpider):
    """'Spider' which is getting data from klwines.com"""

    name = 'klwines'
    #download_delay = 1
    LOGIN = "winetron@protonmail.com"
    PASSWORD = "ilovewine1A"

    # start_urls = ['']

    ##def parse(self, response):
    #    return FormRequest.from_response(
    #        response,
    #        formdata={'onlineid': self.LOGIN,
    #                  'password': self.PASSWORD,
    #                  'reqemail': "Y",
    #                  'submit': "1",
    #                  'x': "0",
    #                  'y': "0"},
    #        callback=self.after_login
    #    )

    # def parse(self, response):
    #    # check login succeed before going on
    #

    def start_requests(self) -> Iterator[Dict]:
        yield Request(
            BASE_URL,
            #'https://www.howsmyssl.com',
            callback=self.before_login
        )

    def before_login(self, _: Response) -> Iterator[Dict]:
        yield Request(
            f'{BASE_URL}/account/login',
            callback=self.login
        )

    def login(self, response: Response) -> Iterator[Dict]:
        token_path = response.xpath(
            '//div[contains(@class,"login-block")]'
            '//input[@name="__RequestVerificationToken"]/@value')
        token = token_path[0].extract()
        return FormRequest.from_response(
            response,
            formxpath='//*[contains(@action,"login")]',
            formdata={'Email': self.LOGIN,
                      'Password': self.PASSWORD,
                      '__RequestVerificationToken': token,
                      'Login.x': "24",
                      'Login.y': "7"},
            callback=self.get_listpages
        )

    def is_not_logged(self, response):
        return "Welcome, John" not in response.body.decode('utf-8')

    def get_listpages(self, response: Response) -> Iterator[Dict]:
        if self.is_not_logged(response):
            self.logger.exception("Login failed")
            return None
        step = 500
        max_items = 15000

        if SCRAPER_PRODUCTS_LIMIT:
            max_items = SCRAPER_PRODUCTS_LIMIT # override max_items setting for debug purposes

        url = f'{BASE_URL}/Products/r?'
        for page in range(0, max_items, step):
            from time import sleep
            sleep(5)
            yield Request(
                f'{url}d=0&r=57&z=False&o=8&displayCount={step}&p={page}',
                meta={'page': page},
                callback=self.parse_listpage,
            )

    def parse_listpage(self, response: Response) -> Iterator[Dict]:
        """Process http response
        :param response: response from ScraPy
        :return: iterator for data
        """
        if self.is_not_logged(response):
            self.logger.exception("Login failed")
            return None
        selector = "//div[contains(concat(' ', @class, ' '), ' result ')]"
        rows = response.xpath(selector)
        links = []
        if not rows:
            raise CloseSpider('no more data')
        else:
            for row in rows:
                if row:
                    if 'auctionResult-desc' not in row.extract():
                        link = row.xpath(
                            'div[@class="result-desc"]/a/@href'
                        ).extract_first()
                        if link:
                            links.append(link)
                        else:
                            logging.exception(
                                f'Link not fount for {row} '
                                f'on page: {response.url}'
                            )
            for link in links:
                absolute_url = BASE_URL + link
                yield Request(absolute_url, callback=self.parse_product, priority=1)

    def parse_product(self, response: Response) -> Iterator[Dict]:
        if self.is_not_logged(response):
            self.logger.exception("Login failed")
            return None
        return WineItem(**ParsedProduct(response).as_dict())


def get_data(tmp_file: IO) -> None:
    process = CrawlerProcess({
        'CONCURRENT_REQUESTS': CONCURRENT_REQUESTS,
        'COOKIES_DEBUG': COOKIES_DEBUG,
        'FEED_FORMAT': 'jsonlines',
        'FEED_URI': f'file://{tmp_file.name}',
        'DOWNLOADER_CLIENT_METHOD': 'TLSv1.2',
        'DOWNLOADER_CLIENTCONTEXTFACTORY': DOWNLOADER_CLIENTCONTEXTFACTORY,
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'
    })

    process.crawl(KLWinesSpider)
    process.start()
