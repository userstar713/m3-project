import re

from typing import Iterator, Dict, IO

from scrapy import FormRequest
from scrapy.http.request import Request
from scrapy.http.response import Response
from scrapy.crawler import CrawlerProcess

from application.scrapers.spider_scraper import get_spider_settings
from application.spiders.base.abstracts.spider import AbstractSpider
from application.spiders.base.abstracts.product import AbstractParsedProduct


BASE_URL = 'https://www.thewineclub.com'


class ParsedProduct(AbstractParsedProduct):

    def __init__(self, r: Response) -> None:
        super().__init__(r)
        self.result['msrp'] = self.get_msrp()

    def get_name(self) -> str:
        name = self.r.xpath(
            '//span[@class="producttitle"]/text()'
        ).extract_first()
        name = name.strip()
        return name

    def get_description(self) -> str:
        description = self.r.xpath(
            '//div[@id="moreinfo"]/p/text()'
        ).getall()
        description = self.clean(''.join(description))
        return description

    def get_sku(self) -> str:
        sku = self.r.xpath(
            '//div[@id="bmg_itemdetail_sku"]/text()'
        ).extract_first()
        return sku and sku.replace('Sku: ', '')

    def get_wine_type(self):
        return self.r.meta['wine_type'].capitalize()

    def get_msrp(self) -> float:
        msrp = self.result['price']
        msrp_selector = self.r.xpath(
            '//span[@class="RegularPrice"]/text()')
        if msrp_selector:
            msrp = msrp_selector.extract_first()
            msrp = msrp.replace('$', '')
            msrp = float(msrp)
        return msrp

    def get_vintage(self) -> str:
        vintage = None
        match = re.match(r'.*([1-3][0-9]{3})', self.name)
        if match:
            vintage = match.group(1)
        else:
            vintage = self.r.xpath(
                '//span[@class="vintage"]/text()'
            ).extract_first()
        return vintage

    def get_price(self) -> float:
        price = self.r.xpath(
            '//span[@class="SalePrice"]/text()'
        ).extract_first()
        price = price.replace('$', '')
        try:
            float_price = float(price)
        except ValueError:
            return "ERROR READING PRICE"
        else:
            return float_price

    def get_image(self) -> str:
        image_link = self.r.xpath(
            '//div[@id="bmg_itemdetail_thumbs"]/ul/li/a/@href'
        ).extract_first()
        if image_link:
            return '/'.join([BASE_URL, image_link])
        return None

    def get_alcohol_pct(self) -> str:
        # return self.r.xpath(
        #     '//span[@class="prodAlcoholPercent_percent"]/text()'
        # ).extract_first()
        return '0'

    def get_varietals(self) -> list:
        return self.r.xpath(
            '//span[text()="Grape Varietal:"]/following::span[1]/a/text()'
        ).extract_first()

    def get_region(self) -> str:
        selector = self.r.xpath(
            '//span[text()="Sub-Region:"]/following::span[1]/a/text()'
        )
        if not selector:
            selector = self.r.xpath(
                '//span[text()="Region:"]/following::span[1]/a/text()'
            )
        if not selector:
            selector = self.r.xpath(
                '//span[text()="Country:"]/following::span[1]/a/text()'
            )
        return selector.extract_first()

    def get_additional(self):
        additional = {
            'bottle_size': 0,
        }
        return additional

    def get_bottle_size(self) -> int:
        size = self.r.xpath(
            '//span[@class="bottlesize"]/text()'
        ).extract_first() or ''
        if 'ML' in size:
            size = int(size.replace('ML', ''))
        elif 'L' in size:
            size = size.replace('L', '')
            size = int(float(size) * 1000)
        return size

    def get_reviews(self) -> list:
        reviews = []
        scores_b = self.r.xpath(
            '//div[@id="review_form_critics"]/b[contains(text(),"points")]')
        if scores_b:
            for score_b in scores_b:
                score_text = score_b.xpath(
                    'text()'
                ).extract_first()
                score_str = re.findall(r'\d+', score_text)[0]
                reviewer_name = re.findall(r'.* \d+', score_text)
                if reviewer_name:
                    reviewer_name = reviewer_name[0].replace(
                        f' {score_str}', '')
                content = score_b.xpath(
                    'following-sibling::text()'
                ).extract_first()
                content = self.clean(content)
                content = content.lstrip('- ')
                reviews.append({
                    'reviewer_name': reviewer_name or '',
                    'score_num': score_str and int(score_str) or None,
                    'score_str': score_str or None,
                    'content': content,
                })
        return reviews

    def get_qoh(self) -> int:

        qoh = self.r.xpath(
            '//span[@class="cartQtyLeft"]/text()'
        ).extract_first()
        qoh = re.findall(r'\d+', qoh)
        return qoh and int(qoh[0])


class TheWineClubSpider(AbstractSpider):
    """'Spider' which is getting data from wine.com"""

    name = 'thewineclub'
    LOGIN = "wine_shoper@protonmail.com"
    PASSWORD = "ilovewine1B"

    def start_requests(self) -> Iterator[Dict]:
        yield Request(
            f'{BASE_URL}/main.asp?request=CHECKOUT&login=Y',
            callback=self.login
        )

    def before_login(self, response: Response):
        pass

    def login(self, response: Response) -> Iterator[Dict]:
        headers = {
            'Accept': '*/*',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-Requested-With': 'XMLHttpRequest',
            'Content-Length': '110',
            'Connection': 'keep-alive',

        }
        yield FormRequest.from_response(
            response,
            formxpath='//form[@id="validForm"]',
            formdata={'username': self.LOGIN,
                      'password': self.PASSWORD,
                      'ISPASS': 'Y',
                      'ajaxrequest': 'SIGNIN',
                      'goto': '',
                      'transid': '',
                      },
            headers=headers,
            callback=self.parse_wine_types
        )

    def is_not_logged(self, response):
        # TODO Check if John is logged
        return "Welcome" not in response.body.decode('utf-8')

    def get_wine_types(self, response: Response) -> list:
        res = []
        allowed_wines = ('Red', 'Rose', 'White', 'Sparkling', 'Dessert')
        selectors = response.xpath("//a[@class='shoplink']")
        for selector in selectors:
            wine_type = selector.xpath('text()').extract_first()
            if wine_type not in allowed_wines:
                continue

            wine_filter = selector.xpath('@href').extract_first()
            res.append((wine_type, None, wine_filter))
        return res

    def get_listpages(self, response: Response) -> Iterator[Dict]:
        wines_num = response.xpath(
            '//form[@name="frmsearch"]//span[@class="results"]/text()'
        ).extract_first()
        wines_num = int(wines_num.split(' ')[-1])
        max_items = 40
        pages_num = wines_num // max_items + (wines_num % max_items > 0)
        for page in range(1, pages_num + 1):
            yield FormRequest.from_response(
                response,
                formxpath='//form[@name="frmsearch"]',
                callback=self.parse_listpage,
                formdata={'pagereq': f'{page}'},
                meta={'wine_type': response.meta['wine_type']},
            )

    def iterate_wine_types(self, response: Response) -> Iterator[Dict]:
        general_data = {
            'reqsearch': 'SEARCH',
            'prod_type': 'W',
            'search': '',
            'country': '0',
            'region': '0',
            'subregion': '0',
            'appellation': '0',
            'sel_variety': '0',
            'selcolor': '0',
            'wine_type': '0',
            'attributes': '0',
            'attributes2': '0',
            'sel_producers': '0',
            'sel_vintage': '',
            'sel_size': '0',
            'rateselect': '',
            'price_range': '0',
        }
        datas = [
            {'selcolor': 'RED'},
            {'selcolor': 'WHITE'},
            {'selcolor': 'ROSE'},
            {'attributes2': 'Dessert'},
        ]
        for data in datas:
            wine_type = data.get('selcolor', data.get('attributes2'))
            data.update(general_data)
            yield FormRequest.from_response(
                response,
                formxpath='//form[@name="frmadvsearch"]',
                formdata=data,
                callback=self.get_listpages,
                dont_click=True,
                meta={'wine_type': wine_type},
            )

    def parse_wine_types(self, response: Response) -> Iterator[Dict]:
        wines_url = f'{BASE_URL}/main.asp?request=ADVSEARCH'
        yield Request(wines_url,
                      callback=self.iterate_wine_types)

    def parse_listpage(self, response: Response) -> Iterator[Dict]:
        """Process http response
        :param response: response from ScraPy
        :return: iterator for data
        """
        selector = '//a[@class="Srch-producttitle"]/@href'
        rows = response.xpath(selector)
        product_links = rows.getall()
        for product_link in product_links:
            product_link = product_link.replace('http://', 'https://')
            yield Request(
                product_link,
                callback=self.parse_product,
                meta={'wine_type': response.meta['wine_type']},
                priority=1)

    def get_product_dict(self, response: Response):
        return ParsedProduct(response).as_dict()

    def get_list_product_dict(self, response: Response):
        raise NotImplementedError


def get_data(tmp_file: IO) -> None:
    settings = get_spider_settings(tmp_file)
    process = CrawlerProcess(settings)
    process.crawl(TheWineClubSpider)
    process.start()


if __name__ == '__main__':
    import os
    current_path = os.getcwd()
    file_name = os.path.join(current_path, 'thewineclub.txt')
    with open(file_name, 'w') as out_file:
        get_data(out_file)
