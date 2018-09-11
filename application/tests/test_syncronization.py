import unittest
from unittest import mock
from application import create_app
from application.tasks.processor import ProductProcessor, Product
from application.db_extension.models import db, DomainAttribute



app = create_app()
with app.app_context():
    domain_attributes = db.session.query(DomainAttribute).all()

class DynamicObject(object):
    pass


class MockFilter(object):  # This is the ONE parameter constructor
    def __init__(self, data):
        self._count = 0
        self._data = data

    def count(self):  # This is the needed Count method
        return self._count

    def first(self):
        return None

    def __iter__(self):
        yield from self._data



class MockQuery(object):  # This is the ONE parameter constructor
    def __init__(self, *args, **kwargs):
        self._data = []
        self._filter = MockFilter(self._data)

    def __iter__(self):
        yield from self._data

    def filter_by(self, *args, **kwargs):
        return self._filter

    def filter(self, *args,
               **kwargs):  # This is used to mimic the query.filter() call
        return self._filter

    def join(self, *args,
             **kwargs):
        return self.__class__()

    def all(self):
        return self._data


class MockDomainAttributes(MockQuery):
    def __init__(self, *args,
                 **kwargs):
        super().__init__(*args,
                       **kwargs)
        self._data = domain_attributes


class MockSession(object):
    def __init__(self):
        self._query = MockQuery()
        self.dirty = []

    def flush(self):
        pass

    def commit(self):
        pass

    def remove(self):
        pass

    def add(self, *args, **kwargs):
        pass

    def execute(self, *args, **kwargs):
        pass

    def query(self, *args, **kwargs):
        if (args
            and hasattr(args[0], '__tablename__')
            and args[0].__tablename__ == 'domain_attributes'):
            return MockDomainAttributes(*args, **kwargs)
        return MockQuery(*args, **kwargs)


@mock.patch('application.db_extension.models.db.session', new=MockSession())
class ProductProcessTest(unittest.TestCase):
    products = [Product.from_raw(
        -1,
        raw_product
    ) for raw_product in [{'price': '19.99',
                           'name': '2013 Giornata "French Camp Vineyard" '
                                   'Paso Robles Aglianico',
                           'vintage': '2013',
                           'msrp': '',
                           'brand': 'Giornata',
                           'region': 'Paso Robles',
                           'varietals': 'Aglianico',
                           'foods': '',
                           'wine_type': 'Red',
                           'body': '',
                           'tannin': '',
                           'image': 'https://dev.vinomogul.com'
                                    '/pub/media/catalog/product/1/2/1277653x.jpg',
                           'characteristics': '',
                           'description': '',
                           'purpose': '',
                           'sku': '1277653',
                           'bottle_size': '750',
                           'qoh': '5',
                           'highlights': '',
                           'single_product_url': 'https://dev.vinomogul.com'
                                                 '/catalog/product/view/id/60979/',
                           'alcohol_pct': '',
                           'drink_from': '',
                           'drink_to': '',
                           'acidity': '',
                           'discount_pct': '',
                           'flaw': '',
                           '_reviews': '[{"reviewer_name":"Vinous","content":"Deep ruby. '
                                       'Lively, spice-laced aromas of dark berries and ca'
                                       'ndied flowers, complemented by subtle vanilla and'
                                       ' woodsmoke notes in the background. Starts off ta'
                                       'ut and firm, then gains weight and breadth, offer'
                                       'ing gently sweet blackberry and cherry flavors an'
                                       'd a touch of candied licorice. Finishes juicy and'
                                       ' long, with building tannins coming in very slowl'
                                       'y. This wine is tighter than the Luna Matta bottl'
                                       'ing and a bit less come-hither in character, at l'
                                       'east for now. (JR)","score_str":"91"}]'}]]

    @mock.patch('application.tasks.synchronization.get_products_for_source_id')
    def test_syncronization(self, get_products_for_source_id_mock):
        get_products_for_source_id_mock.return_value = self.products
        from application.tasks.synchronization import start_synchronization
        with app.app_context():
            start_synchronization(12)
        assert True

    def test_process_single_product(self):
        with app.app_context():
            pp = ProductProcessor()
            pp.process(self.products[0])
        assert True
