from scrapy.exceptions import DropItem


class PricePipeline(object):

    def process_item(self, item, _):
        self._check_bottle_size(item):

    def _check_bottle_size(self, bottle_size: int) -> bool:
        if item['bottle_size'] != 750:
            raise DropItem(f'Ignoring bottle size: {item["bottle_size"]}')
