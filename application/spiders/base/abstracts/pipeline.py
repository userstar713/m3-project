import re
from abc import ABC
from scrapy.exceptions import DropItem
from scrapy.http.request import Request
from sqlalchemy import text
from application.db_extension.models import (
    db,
    Source,
    MasterProductProxy,
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
        self._check_sku(item)
        return item

    def _check_sku(self, item: dict):
        if not item['sku']:
            raise DropItem(
                f'Skipping product with missing SKU: {item}')

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

    def get_original_qoh(self, source_id, master_product_id) -> int:
        q = text(
            "SELECT ov.attr_json "
            "FROM ("
            "      SELECT master_product_id,"
            "             jsonb_agg(json_build_object('attr_id', attribute_id, 'value', values)) attr_json"
            "      FROM ("
            "            SELECT master_product_id,(jsonb_array_elements(value#>'{{attributes}}')->'attr_id')::text::integer attribute_id, jsonb_array_elements(value#>'{{attributes}}')->'values'->0->'v_orig' AS values"
            "            FROM out_vectors"
            "            WHERE sequence_id=(SELECT max(id) FROM pipeline_sequence WHERE source_id=:source_id)"
            "            AND master_product_id=:master_product_id) s "
            "WHERE attribute_id=21 "
            "GROUP BY master_product_id) ov"
        )
        res = db.session.execute(
            q,
            params={'source_id': source_id,
                    'master_product_id': master_product_id}
        ).fetchone()
        if not res:
            return 0
        original_values = dict(res)
        qoh = round(original_values['attr_json'][0]['value'])
        return qoh

    def process_item(self, item, spider):
        if not item['name']:
            raise DropItem(f'Skipping invalid item {item}')
        qoh = item['qoh']
        if qoh is None:
            source_id = spider.settings['SOURCE_ID']
            master_product = MasterProductProxy.get_by(
                name=item['name'],
                source_id=source_id,
            )
            if master_product:
                qoh = self.get_original_qoh(source_id, master_product.id)
                qoh_threshold = db.session.query(
                    Source.min_qoh_threshold
                ).filter_by(
                    id=source_id
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
            else:
                self.crawler.engine.crawl(
                    Request(
                        url=item['single_product_url'],
                        callback=self.parse_detail_page,
                        meta={'item': item},),
                    spider,
                )
                raise DropItem(
                    'Opening product detail page to add new product.')
        return item

    def parse_detail_page(self, response):
        pass

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
