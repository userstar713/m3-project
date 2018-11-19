import logging
import re

from typing import Iterator, Dict, IO

from scrapy import FormRequest, Selector
from scrapy.http.request import Request
from scrapy.http.response import Response
from scrapy.crawler import CrawlerProcess

from application.scrapers.spider_scraper import get_spider_settings
from application.spiders.base.abstracts.spider import AbstractSpider
from application.spiders.base.abstracts.product import AbstractParsedProduct


BASE_URL = 'https://www.klwines.com'


class ParsedListPageProduct(AbstractParsedProduct):

    def get_sku(self):
        pass

    def get_name(self) -> str:
        return self.s.xpath(
            'div[@class="result-desc"]/a/text()'
        ).extract_first()

    def get_vintage(self):
        pass

    def get_price(self) -> float:
        s = self.clean(self.s.xpath(
            'div[@class="result-info"]/span/span/span/strong/text()'
        ).extract_first())
        s = s.replace('$', '').replace(',', '')
        try:
            float_s = float(s)
        except ValueError:
            return "ERROR READING PRICE"
        else:
            return float_s

    def get_image(self):
        pass

    def get_additional(self):
        return {}

    def get_bottle_size(self):
        pass

    def get_qoh(self):
        pass

    def get_reviews(self):
        pass


class ParsedProduct(AbstractParsedProduct):

    def get_sku(self) -> str:
        value = self.r.xpath(
            '//span[@class="SKUInformation"]/text()'
        ).extract()[0]
        value = value.replace('SKU ', '')
        return value

    def get_name(self) -> str:
        return self.clean(self.r.xpath(
            '//div[@class="result-desc"]/h1/text()'
        )[0].extract())

    def get_vintage(self) -> str:
        res = ''
        match = re.match(r'.*([1-3][0-9]{3})', self.name)
        if match:
            res = match.group(1)
        return res

    def get_price(self) -> float:
        s = self.clean(self.r.xpath(
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
        return self.r.xpath('//img[@class="productImg"]/@src')[
            0].extract()

    def get_additional(self):
        additional = {
            'varietals': [],
            'alcohol_pct': None,
            'name_varietal': None,
            'region': None,
            'description': None,
            'other': None,
        }
        rows = self.r.xpath('//div[@class="addtl-info-block"]/table/tr')
        detail_xpath_value = 'td[@class="detail_td"]/h3/text()'
        title_xpath = 'td[@class="detail_td1"]/text()'
        for row in rows:
            title = self.clean(row.xpath(title_xpath).extract()[0])
            if title == "Alcohol Content (%):":
                value = self.clean(row.xpath(
                    'td[@class="detail_td"]/text()').extract()[0])
                additional['alcohol_pct'] = value
            else:
                values = row.xpath(detail_xpath_value).extract()
                value = values and values[0].replace(" and ", " ")
                if title == "Varietal:":
                    description = self.clean(
                        row.xpath(
                            'td[@class="detail_td"]/text()').extract()[1])
                    additional['description'] = description

                    additional['name_varietal'] = value
                    if value:
                        additional['varietals'].append(value)
                elif title in ("Country:",
                               "Sub-Region:",
                               "Specific Appellation:"):
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
            reviewer_name = self.clean(
                ''.join(rp.xpath('text()').extract())
            )
            content = self.clean(''.join(text.xpath('text()').extract()))

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
                    score = self.clean(
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
        rows = self.r.xpath(
            '//div[@class="inventory clearfix"]/div[@class="column"]//tr')
        qoh = 0
        for row in rows[1:]:
            qty = self.clean(row.xpath('td/text()')[-1].extract())
            qty = qty.replace('>', '').replace('<', '')
            qoh += int(qty)
        return qoh

    def as_dict(self) -> Dict:
        return self.result


class KLWinesSpider(AbstractSpider):
    """'Spider' which is getting data from klwines.com"""

    name = 'klwines'
    LOGIN = "wine_shoper@protonmail.com"
    PASSWORD = "ilovewine1B"

    def start_requests(self) -> Iterator[Dict]:
        yield Request(
            BASE_URL,
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
            callback=self.parse_wine_types
        )

    def is_not_logged(self, response):
        return "Welcome, John" not in response.body.decode('utf-8')

    def get_wine_types(self, response: Response) -> list:
        res = []
        rows = response.xpath(
            '//ul[@id="category-menu-container-ProductType"]/li/a')[::-1]
        for row in rows:
            wine_filter = row.xpath('@href').extract()[0]
            wine_type = row.xpath('@title').extract()[0]
            wine_type = wine_type.replace('Wine - ', '').lower()
            wines_total = row.xpath('span[2]/text()').extract()[0]
            wines_total = int(wines_total[1:-1])
            if 'misc' in wine_type:
                continue
            res.append((wine_type, wines_total, wine_filter))
        return res

    def get_listpages(self, response: Response) -> Iterator[Dict]:
        wine_types = self.get_wine_types(response)
        step = 500
        items_scraped = 0
        for (wine_type, wines_total, wine_filter) in wine_types:
            wine_filter = wine_filter.replace('limit=50', f'limit={step}')
            wine_filter = wine_filter.replace('&offset=0', '')
            if wines_total % step or wines_total < step:
                wines_total += step

            for offset in range(0, wines_total, step):

                items_scraped += offset
                url = f'{wine_filter}&offset={offset}'
                offset += step
                yield Request(
                    f'{BASE_URL}{url}',
                    meta={'wine_type': wine_type},
                    callback=self.parse_listpage,
                )

    def parse_wine_types(self, response: Response) -> Iterator[Dict]:
        if self.is_not_logged(response):
            self.logger.exception("Login failed")
            yield
        else:
            wines_url = f'{BASE_URL}/Wines'
            yield Request(wines_url,
                          callback=self.get_listpages)

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
                "//div[contains(concat(' ', @class, ' '), ' result ')]")
            links = []
            for row in rows:
                if full_scrape:
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

    def get_product_dict(self, response: Response):
        return ParsedProduct(response).as_dict()

    def get_list_product_dict(self, response: Response):
        return ParsedListPageProduct(response).as_dict()


def get_data(tmp_file: IO) -> None:
    settings = get_spider_settings(tmp_file)
    process = CrawlerProcess(settings)
    process.crawl(KLWinesSpider)
    process.start()


if __name__ == '__main__':
    import os
    current_path = os.getcwd()
    file_name = os.path.join(current_path, 'klwines.txt')
    with open(file_name, 'w') as out_file:
        get_data(out_file)
