import logging

from typing import NamedTuple, Iterator, Dict, IO
from itertools import count

from scrapy.http.request import Request
from scrapy.http.response import Response
from scrapy.selector import Selector
from scrapy.crawler import CrawlerProcess
from scrapy.exceptions import CloseSpider

from .base import BaseSpider

CONCURRENT_REQUESTS = 16


class ParsedRow(NamedTuple):
    name: str

    @staticmethod
    def from_raw(s: Selector) -> 'ParsedRow':
        name = s.xpath('div[@class="result-desc"]/a/text()').extract_first()
        if not name:
            logging.exception(f'Error while processing: {s.extract()}')
            return None
        return ParsedRow(
            name=name
        )

    def as_dict(self) -> Dict:
        """
        `_as_dict` private method proxy
        :return: named tuple converted to dict
        """
        return self._asdict()


class KLWinesSpider(BaseSpider):
    """'Spider' which is getting data from klwines.com"""

    name = 'historical_prices'

    def start_requests(self) -> Iterator[Request]:
        """Make a request for a page with wine data
        """
        step = 500
        url = 'https://www.klwines.com/Products/r?'
        for page in count(step=step):
            yield Request(
                f'{url}d=0&r=57&z=False&o=8&displayCount={step}&p={page}',
                meta={'page': page}
            )

    def parse(self, response: Response) -> Iterator[Dict]:
        """Process http response
        :param response: response from ScraPy
        :return: iterator for data
        """
        selector = "//div[contains(concat(' ', @class, ' '), ' result ')]"
        rows = response.xpath(selector)
        if not rows:
            raise CloseSpider('no more data')
        else:
            for row in rows:
                if row:
                    try:
                        yield ParsedRow.from_raw(row).as_dict()
                    except ValueError:
                        logging.exception('Error parsing row {row}')


def get_data(tmp_file: IO) -> None:
    """Получить исторические цены с NASDAQ и сохранить их в файл.
    Данные сохраняются в формате jsonlines - по одному json-объекту в каждой строке.
    :param tmp_file: временный файл, куда полученные данные будут сохранены в формате jsonlines
    """
    process = CrawlerProcess({
        'CONCURRENT_REQUESTS': CONCURRENT_REQUESTS,
        'FEED_FORMAT': 'jsonlines',
        'FEED_URI': f'file://{tmp_file.name}',
    })

    process.crawl(KLWinesSpider)
    process.start()
