import re
from abc import ABC
from scrapy.exceptions import DropItem
from scrapy.http.request import Request
from application.db_extension.models import (
    db,
    Source,
    SourceProductProxy,
    SourceLocationProductProxy
)


class BaseFilterPipeline(ABC):
    """Abstract class to filter crawled products from the web sites.
    Products are filtered after the scrape using Scrapy Pipeline Middleware.
    A product should be ignored if:
        * bottle_size != 750 ml;
        * product has no image or has generic placeholder set as an image;
        * is not available to but(qoh=0);
        * is not in the stock yet (is pre-arrival);
        * is not a single bottle (is multipack)
    """

    IGNORED_IMAGES = []

    def process_item(self, item: dict, _):
        """Check that the scraped product met all the requirements
        and return ir if passed, DropItem is raised otherwise"""
        self._check_bottle_size(item)
        self._check_product_image(item)
        self._check_qoh(item)
        self._check_prearrival(item)
        self._check_multipack(item)
        return item

    def _check_bottle_size(self, item: dict):
        if item['bottle_size'] != 750:
            raise DropItem(
                f'Skipping product with bottle size: {item["bottle_size"]}')

    def _check_product_image(self, item: dict):
        image = item['image']
        if not image or 'default_bottle' in image:
            raise DropItem(
                f'Skipping product with ignored image: {item["name"]}')
        relative_image = image.split('/')[-1]
        if relative_image in self.IGNORED_IMAGES:
            raise DropItem(
                f'Skipping product with ignored image: {item["name"]}')

    def _check_qoh(self, item: dict):
        if not item['qoh']:
            raise DropItem(f'Skipping product with no qoh: {item["name"]}')

    def _check_prearrival(self, item: dict):
        regexp = re.compile('.*(Pre-ArrivaL|PRE-ORDER|Pre-Sale).*',
                            re.IGNORECASE)
        is_prearrival = bool(regexp.match(item['name']))
        if is_prearrival:
            raise DropItem(f'Skipping pre-arrival: {item["name"]}')

    def _check_multipack(self, item: dict):
        regex = re.compile(r'.*(\d Pack).*', re.IGNORECASE)
        if bool(regex.match(item['name'])):
            raise DropItem(f'Ignoring multipack product: {item["name"]}')


class BaseIncPipeline(ABC):

    def __init__(self, crawler):
        self.crawler = crawler

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_item(self, item, spider):
        qoh = item['qoh']
        if qoh is None:
            source_product = SourceProductProxy.get_by(
                name=item['name'],
                source_id=spider.settings['SOURCE_ID'],
            )
            if source_product:
                src_location_product = SourceLocationProductProxy.get_by(
                    source_product_id=source_product.id,
                )
                if src_location_product:
                    qoh = src_location_product.qoh
                    qoh_threshold = db.session.query(
                        Source.min_qoh_threshold
                    ).filter_by(
                        id=spider.settings['SOURCE_ID']
                    ).scalar()
                    if qoh <= qoh_threshold:
                        self.crawler.engine.crawl(
                            Request(
                                url=item['single_product_url'],
                                callback=self.update_qoh,
                                meta={'item': item},),
                            spider,
                        )
                        raise DropItem(
                            'Opening product detail page to read the qoh.')
        return item

    def update_qoh(self, response):
        item = response.meta.get('item')
        item['qoh'] = self.get_qoh(response)
        yield item

    def get_qoh(self, response):
        """
        Make http request to single_product_url to read product qoh from
        the form view. Make sure to call this if qoh is not available in
        the list view only.
        """
        pass
