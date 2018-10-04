import json
import re
from funcy import log_durations
from datetime import datetime
from application.caching import cache
from application.db_extension.models import (db,
                                             DomainAttribute,
                                             MasterProductProxy,
                                             SourceProductProxy,
                                             SourceAttributeValue,
                                             DomainReviewer,
                                             SourceReview,
                                             source_location_product,
                                             SourceLocationProductProxy)
from application.db_extension.models import db
from typing import NamedTuple
from application.db_extension.routines import (
    get_default_category_id,
    attribute_lookup)
from application.utils import listify, get_float_number
from application.logging import logger
from sqlalchemy.sql.expression import func

from .helpers import DATATYPES, remove_diacritics


def filter_brands(attributes: list) -> list:
    return [a for a in attributes if a['code'] == 'brand']


@cache.memoize()
def get_nlp_ngrams():
    # TODO deprecated???
    rows = db.session.execute(
        """SELECT text_value_processed target_text, 
                 replace(text_value_processed,'_',' ') source_text
           FROM domain_dictionary 
           WHERE is_require_ngram=TRUE
           ORDER BY char_length(text_value_processed) DESC;""")
    return [dict(row) for row in rows]


@cache.memoize(timeout=60 * 60)
def get_process_product_attributes():
    rows = db.session.query(
        DomainAttribute.code
    )
    return [row[0] for row in rows]


def get_nlp_ngrams():
    logger.info('get_nlp_ngrams')
    q = """SELECT text_value_processed target_text,
                   replace(text_value_processed,'_',' ') source_text
            FROM domain_dictionary WHERE is_require_ngram=TRUE
            ORDER BY char_length(text_value_processed) DESC"""
    rows = db.session.execute(q)
    return {row.target_text: row.source_text for row in rows}


@cache.memoize(timeout=200)
def get_nlp_ngrams_regex():
    ngrams = get_nlp_ngrams()
    regex = re.compile("(%s)" % "|".join(map(re.escape, ngrams.keys())))
    return ngrams, regex


@cache.memoize(timeout=200)
def replace_with_nlp_ngrams(cleaned_string):
    ngrams, regex = get_nlp_ngrams_regex()
    return regex.sub(
        lambda mo: ngrams[mo.string[mo.start():mo.end()]], cleaned_string
    )

    # for nlps in nlp_synonyms:
    #    # pattern = r'\b{}\b'
    #    # rg = pattern.format(nlps['source_text'])
    #    # cleaned_string = re.sub(rg, nlps['target_text'], cleaned_string, flags=re.I)
    #    # Note: because this is simple replacement, don't need regex and much much faster
    #    # cleaned_string = cleaned_string.replace(nlps['source_text'], nlps['target_text'])
    #    cleaned_string = re.sub(r"\b%s\b" % nlps['source_text'], nlps['target_text'], cleaned_string)

    # return cleaned_string


def get_domain_taxonomy_node_id_from_dict(attribute_code,
                                          attribute_value):
    """
    This function should return a single integer
    """
    # If result is empty array, return -1
    # else, get the value for results[0].node_id  (node_id from the first object in array) and return that
    # To see format of response to above API, try this as body:
    # {"text":"Latour", "category":1, "attr_codes":["brand"], "attr_restriction":null}
    # Response --> {"attributes":
    # [{"code":"brand",
    #  "end":0,
    # "node_id":3161,
    #  "original":"Latour",
    # "start":0,
    #  "value":"Chateau Latour"}]}
    # We only care about node_id
    # node_id = 12345;
    # return node_id;
    if attribute_code == 'brand':
        #replace_with_nlp_ngrams(attribute_value.lower())
        attribute_value = attribute_value # Fixme
    attr_result = attribute_lookup(sentence=attribute_value)
    if attr_result:
        result = attr_result[0]['node_id']
    else:
        result = -1
    # logger.debug("*** NODE ID: {}-{}-{} {} ".format(category_id, attribute_code, attribute_value, result))
    return result



class DomainReviewers:
    data: list = []

    def initialize(self):
        _data = db.session.query(DomainReviewer.id,
                                 DomainReviewer.name,
                                 DomainReviewer.aliases)
        for d in _data:
            self.data.append({
                'id': d.id,
                'name': d.name,
                'aliases': d.aliases,
            })

    def get_id_by_name_or_alias(self, name_or_alias):
        for item in self.data:
            if item['name'] == name_or_alias or name_or_alias in item[
                'aliases']:
                return item['id']
        return None


# This routine is used to process "varietals" field.
# e.g. if the product name contains "2017 Chateau Kim Cabernet Sauvignon"
# and the original varietals contains "Cab, Merlot, Malbect", it will replace varietals with Cabernet

def process_varietals(product_name, attribute_code, value):
    # logger.debug('process_varietals: {} {}'.format(product_name, attribute_code))
    name_varietals = attribute_lookup(sentence=product_name)
    new_value = value
    if len(name_varietals) > 0:
        new_value = ', '.join(varietal['value'] for varietal in name_varietals)
    return new_value


def prepare_reviews(reviews: list) -> list:
    if isinstance(reviews, str):
        reviews = reviews.strip()
    if not reviews:
        return []
    if isinstance(reviews, str) and reviews.startswith('['):
        try:
            reviews = json.loads(reviews)
        except json.JSONDecodeError:
            reviews = []
    reviews = listify(reviews)
    return reviews


drc = DomainReviewers()


class Product(NamedTuple):
    name: str
    source_id: int
    reviews: list
    price: str
    vintage: str
    msrp: str
    brand: str
    region: str
    varietals: str
    foods: str
    wine_type: str
    body: str
    tannin: str
    image: str
    characteristics: str
    description: str
    purpose: str
    sku: str
    bottle_size: str
    qoh: str
    highlights: str
    single_product_url: str
    alcohol_pct: str
    drink_from: str
    drink_to: str
    acidity: str
    discount_pct: str
    flaw: str

    @staticmethod
    def from_raw(source_id: int, product: dict) -> 'Product':
        try:
            _product = {
                'source_id': source_id,
                'name': product['name'],
                'reviews': product.get('reviews'),
                'price': product.get('price'),
                'vintage': product.get('vintage'),
                'msrp': product.get('msrp'),
                'brand': product.get('brand'),
                'region': product.get('region'),
                'varietals': product.get('varietals'),
                'foods': product.get('foods'),
                'wine_type': product.get('wine_type'),
                'body': product.get('body'),
                'tannin': product.get('tannin'),
                'image': product.get('image'),
                'characteristics': product.get('characteristics'),
                'description': product.get('description'),
                'purpose': product.get('purpose'),
                'sku': product.get('sku'),
                'bottle_size': product.get('bottle_size'),
                'qoh': product.get('qoh'),
                'highlights': product.get('highlights'),
                'single_product_url': product.get('single_product_url'),
                'alcohol_pct': product.get('alcohol_pct'),
                'drink_from': product.get('drink_from'),
                'drink_to': product.get('drink_to'),
                'acidity': product.get('acidity'),
                'discount_pct': product.get('discount_pct'),
                'flaw': product.get('flaw'),
            }
        except AttributeError as e:
            logger.critical(f'Error while converting raw product {product}')
            raise e
        else:
            return Product(**_product)

    def as_dict(self) -> dict:
        return self._asdict()


class BulkAdder:
    def __init__(self, model: db.Model, threshold: int = 10000) -> None:
        assert hasattr(model, 'bulk_insert_do_nothing')
        self._model = model
        self._threshold = threshold
        self._data = []

    def add(self, data: dict) -> None:
        self._data.append(data)
        if len(self._data) == self._threshold:
            self.flush()

    def flush(self) -> None:
        self._model.bulk_insert_do_nothing(
            self._data
        )
        self._data = []


class DomainDictionaryCache:
    cache = {}

    @staticmethod
    def _get_key(*args):
        return "|".join([str(a) for a in args])

    def get_or_set(self, category_id, attribute_code, attribute_value):
        key = self._get_key(category_id, attribute_code, attribute_value)
        if key not in self.cache:
            # _value = db.session.query(
            #    DomainDictionary.entity_id
            # ).filter_by(
            #    attribute_id=da_id,
            #    text_vector=func.to_tsvector('english', value)
            # ).first()
            _value = get_domain_taxonomy_node_id_from_dict(
                attribute_code=attribute_code,
                attribute_value=attribute_value
            )
            self.cache[key] = _value
            return _value
        else:
            # logger.debug(f"Node ID for {key} is {self.cache[key]}")
            return self.cache[key]


class ProductProcessor:
    def __init__(self):
        self.domain_attributes = {row.code: row.__dict__ for row in
                                  db.session.query(DomainAttribute)}
        self.category_id = get_default_category_id()
        self.to_insert_SAV = []
        self.to_insert_reviews = []
        self.master_product_id = None
        self.source_product_id = None
        self.product = None
        self.sav_bulk_adder = BulkAdder(SourceAttributeValue)
        self.review_bulk_adder = BulkAdder(SourceReview)
        # self.nlp_synonyms = get_nlp_ngrams() TODO deprecated??

    def flush(self):
        self.sav_bulk_adder.flush()
        self.review_bulk_adder.flush()

    def replace_with_nlp_ngrams(self, cleaned_string):
        for nlps in self.nlp_synonyms:
            # pattern = r'\b{}\b'
            # rg = pattern.format(nlps['source_text'])
            # cleaned_string = re.sub(rg, nlps['target_text'], cleaned_string, flags=re.I)
            # Note: because this is simple replacement, don't need regex and much much faster
            # cleaned_string = cleaned_string.replace(nlps['source_text'], nlps['target_text'])
            cleaned_string = re.sub(r"\b%s\b" % nlps['source_text'],
                                    nlps['target_text'], cleaned_string)
        return cleaned_string

    def generate_review(self, data):
        domain_reviewer_id = drc.get_id_by_name_or_alias(
            name_or_alias=data.get('reviewer_name', 'Unknown')
        )
        if not data.get('reviewer_name'):
            reviewer_name = "Unknown"
        else:
            reviewer_name = data['reviewer_name']
        reviewer_id = domain_reviewer_id if domain_reviewer_id else None
        score_str = data.get('score_str', '')
        score_num = data.get('score_num',
                             int(score_str) if str(score_str).isdigit() else 0)
        # Weed out anomalies
        if score_num < 70:
            score_num = None

        return {
            'reviewer_id': reviewer_id,
            'reviewer_name': reviewer_name,
            'content': data.get('content'),
            'score_str': score_str,
            'score_num': score_num,
            'source_product_id': self.source_product_id,
            'source_id': self.product.source_id,
        }

    def generate_sav_list(self,
                          value,
                          da_id,
                          da_code,
                          datatype,
                          value_key):
        # Iterate all the elements if value is of type array.
        #  Each element will be treated as a separate property.
        result = []
        value = listify(value)
        for prop in value:
            # If product property is an array and datatype = node_id
            # then find the node_id_value again for each item.
            if datatype == 'node_id':
                val = get_domain_taxonomy_node_id_from_dict(da_code,
                                                            remove_diacritics(
                                                                prop)
                                                            if prop else '')
            else:
                val = prop
            if datatype == 'node_id' and val == -1:
                # If we do not find the node_id for some value then lets not insert it.
                continue
            result.append({
                'source_product_id': self.source_product_id,
                'attribute_id': da_id,
                'datatype': datatype,
                'source_id': self.product.source_id,
                'master_product_id': self.master_product_id,
                'value_integer': None,
                'value_node_id': None,
                'value_float': None,
                'value_boolean': None,
                'value_text': None,
                value_key: val
            })
        return result

    def create_master_and_source(self):
        master_product, _ = MasterProductProxy.get_or_create(
            name=self.product.name,
            source_id=self.product.source_id,
            source_identifier='TEST',  # TODO  is it required?
            category_id=self.category_id
        )
        db.session.commit()
        self.master_product_id = master_product.id

        source_product, _ = SourceProductProxy.get_or_create(
            name=self.product.name,
            source_id=self.product.source_id,
            master_product_id=self.master_product_id)
        db.session.commit()
        self.source_product_id = source_product.id

    def prepare_process_product(self, name: str) -> dict:
        """
        Cleanup product name, look for brand and non attribute words
        :param name
        :return: dictionary
        """
        # logger.info("preparing product started")
        # pipeline_attribute_lookup already calls cleanup_string() below
        # name = self.replace_with_nlp_ngrams(name.lower())
        unaccented_name = remove_diacritics(name)
        # processed = cleanup_string(name, check_synonyms=False)

        # Test with fixed codes here. If works, then add new column to domain attributes and use that to filter attributes
        # attr_codes = []
        # attr_codes = ['vintage','brand','region','quality_level','varietals','wine_type','styles','bottle_size','is_blend','sweetness']
        # attr_codes = get_process_product_attributes()

        attributes = attribute_lookup(sentence=unaccented_name)
        brands = filter_brands(attributes)
        if len(brands) > 1:  # what to do if more than one brand returned?
            msg = "prepare_process_product returns more than one brand!"
            raise ValueError(msg)
        brand_node_id = brands[0]['node_id'] if brands else None
        # logger.info("preparing product finished")
        return {
            'processed': unaccented_name,
            'brand_node_id': brand_node_id,
            'extra_words': []
        }

    def process_master_product(self) -> dict:
        prepared = self.prepare_process_product(self.product.name)
        upd_data = {
            'processed_name': prepared['processed'],
            'brand_node_id': prepared['brand_node_id']
        }
        if prepared['extra_words']:
            non_attribute_words = ' '.join(prepared['extra_words'])
            upd_data['non_attribute_words'] = non_attribute_words
            upd_data['non_attribute_words_vector'] = func.to_tsvector(
                'english', non_attribute_words)
            upd_data['updated'] = datetime.now()
        # logger.info("updating master_product")
        q = db.session.query(MasterProductProxy).filter_by(
            id=self.master_product_id)
        q.count()
        q.update(upd_data, synchronize_session='fetch')
        db.session.commit()
        # logger.info("process product finsihed")
        return {
            'name': self.product.name,
            'processed': prepared['processed'],
            'brand_node_id': prepared['brand_node_id'],
            'extra_words': prepared['extra_words']
        }

    @log_durations(logger.debug, unit='ms')
    def process(self, product: Product):
        self.product = product
        self.create_master_and_source()

        default_location_id = 1  # TODO fix that

        price = get_float_number(product.price)
        if not price or price < 1:
            logger.error(
                'webhook - ERROR - Invalid price, p={}'.format(product))
            return

        qoh = int(product.qoh)
        price_int = round(price * 100)

        slp = SourceLocationProductProxy.get_by(
            source_product_id=self.source_product_id,
            source_location_id=default_location_id
        )
        if slp:
            slp.price = price
            slp.qoh = qoh
            slp.price_int = price_int
            db.session.commit()
        else:
            db.session.add(source_location_product.SourceLocationProduct(
                source_product_id=self.source_product_id,
                source_location_id=default_location_id,
                price=price,
                qoh=qoh)
                # price_int=price_int)
            )
            db.session.commit()

        for j, (da_code, value) in enumerate(self.product.as_dict().items()):
            if not value or da_code in ['price', 'qoh']:
                # Do nothing if an attribute does not have any value OR it is
                # either 'price' OR 'qoh' because we have already put the qoh
                # and price in source_location_products
                continue
            da = self.domain_attributes.get(da_code)
            # Skip the property
            # if there is no domain attribute record for the property
            if not da:
                continue
            # If domainAttribute is having has_taxonomy true then the dataType
            # is node_id and we have to fetch the node_id value from
            # domain_taxonomy_node table. Otherwise use the actual datatype.
            datatype = 'node_id' if da['has_taxonomy'] else da['datatype']
            # replace original varietals if varietal in name
            if da_code == 'varietals':
                value = process_varietals(product_name=self.product.name,
                                          attribute_code=da_code,
                                          value=value)

            # `sav` is a `source attribute value`
            assert self.source_product_id
            sav_list = self.generate_sav_list(value=value,
                                              da_id=da[
                                                  'id'],
                                              da_code=da_code,
                                              datatype=datatype,
                                              value_key=
                                              DATATYPES[
                                                  datatype])

            for sav in sav_list:
                self.sav_bulk_adder.add(sav)

            brand_id = value if da_code == 'brand' else None
            # just_inserted = not bool(master_product.brand_node_id)
            # TEMPORARY COMMENT THIS OUT. WE SHOULD ONLY PROCESS MASTER PRODUCT WHEN WE HAVE
            #  JUST INSERTED A NEW MASTER PRODUCT. NOT ON UPDATES
            # if just_inserted:
            self.process_master_product()
            #    just_inserted = False

        raw_reviews = prepare_reviews(product.reviews)
        to_insert_reviews = [
            self.generate_review(data=r)
            for r in raw_reviews if r
        ]
        for review in to_insert_reviews:
            self.review_bulk_adder.add(review)
        return self.master_product_id
