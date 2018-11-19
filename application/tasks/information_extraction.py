import re
from collections import defaultdict
from datetime import datetime

from sqlalchemy.dialects import postgresql

from application.stopwords import STOPWORDS
from application.db_extension.routines import (domain_attribute_lookup,
                                               get_description_contents_data)
from application.db_extension.models import db
from application.db_extension.models import (PipelineAttributeValue,
                                             PipelineReviewContent)
from application.db_extension.models  import DomainAttribute, DomainTaxonomyNode
from application.db_extension.models import MasterProductProxy, SourceProductProxy
from application.utils import remove_duplicates, split_to_sentences
from application.summarization import summarize
from logging import getLogger

logger = getLogger(__name__)

class SqlAlchemyBulkInserter:
    model = None
    max_length = 5000

    def __init__(self, sequence_id):
        self._data = []
        self._sequence_id = sequence_id

    def bulk_insert(self):
        if self._data:
            stmt = postgresql.insert(self.model).values(self._data)
            db.session.execute(stmt)
            db.session.commit()
            self._data = []

    def insert(self, value: dict):
        self._data.append(value)
        if len(self._data) > self.max_length:
            self.bulk_insert()


class PipelineAttributeValueBulkInserter(SqlAlchemyBulkInserter):
    model = PipelineAttributeValue

    def insert(self, value: dict):
        prepared = {
            'sequence_id': None,
            'master_product_id': None,
            'source_id': None,
            'source_location_id': None,
            'attribute_id': None,
            'datatype': None,
            'value_node_id': None,
            'value_integer': None,
            'value_float': None,
            'value_boolean': None,
            'value_text': None,
        }
        for k, v in value.items():
            if k in prepared:
                prepared[k] = v
            else:
                continue
        super().insert(prepared)

    def insert_data(self, data):
        for item in data:
            item['sequence_id'] = self._sequence_id
            self.insert(item)

patterns_presents = lambda x: x.extract_content_support \
                              and 'regex_patterns' in x.extract_content_support

class PipelineExtractor:
    def __init__(self, source_id, sequence_id,
                 debug_product_search_str=None):
        self.source_id = source_id
        self.sequence_id = sequence_id
        self.debug_product_search_str = debug_product_search_str

        logger.info("pipeline_information_extraction: starts, sourceId=" + str(
            source_id)  + ", sequence_id==" + str(sequence_id))

        domain_attributes = db.session.query(DomainAttribute).all()
        if not domain_attributes:
            raise ValueError(f'{DomainAttribute.__tablename__} is empty!' )
        self.rating_attribute_id = \
            [attr.id for attr in domain_attributes if
             attr.code == 'rating'][0]
        self.short_desc_attribute_id = \
            [attr.id for attr in domain_attributes if
             attr.code == 'short_desc'][0]

        self.attrdict = {a.id: a.__dict__ for a in domain_attributes}
        self.domain_attributes_for_extract = [attr for attr in
                                              domain_attributes if
                                              attr.should_extract_values]
        self.domain_attributes_for_product_name_extract = [attr for attr in
                                                           domain_attributes if
                                                           attr.should_extract_from_name]

        self.extract_content = list(
            filter(patterns_presents,
                   self.domain_attributes_for_extract
                   ))
        self.domain_attribute_codes = [attr.code for attr in
                                       self.domain_attributes_for_extract]
        self.extract_content_for_product_name = list(
            filter(patterns_presents,
                   self.domain_attributes_for_product_name_extract))
        self.domain_attribute_codes_for_product_name = [attr.code
                                                        for attr in
                                                        self.domain_attributes_for_product_name_extract]

        self.products = list(
            db.session.query(MasterProductProxy.id, MasterProductProxy.name)
                .join(SourceProductProxy.master_product)
                .filter(MasterProductProxy.source_id == source_id,
                        SourceProductProxy.source_id == source_id))
        self.pav_bulk_inserter = PipelineAttributeValueBulkInserter(
            sequence_id=sequence_id)
        self.description_contents_data = None
        self.review_contents_all = defaultdict(list)
        review_contents_from_db = db.session.query(
            PipelineReviewContent
        ).filter_by(source_id=source_id,
                    sequence_id=sequence_id)
        for r in review_contents_from_db:
            self.review_contents_all[r.master_product_id].append(r)

    def __repr__(self):
        return f'PipelineExtractor for source_id {self.source_id}'

    def process_product(self, product):
        short_descs_data = []
        res = []
        review_contents = self.review_contents_all.get(product.id, [])

        # logger.debug("pipeline_info_extraction: numReviewContents=" + str(len(review_contents)))

        # EXTRACT FROM REVIEWS
        review_score = 0
        review_count = 0
        for row in review_contents:
            if row.review_score:
                review_score += row.review_score
                review_count += 1
            if row.content:
                tmp_res = []
                result = self.extract_from_reviews(row.content, product.id,
                                                   track_to=tmp_res)
                short_descs_data.append({
                    'reviewer': row.reviewer_name,
                    'rating': row.review_score,
                    'sentences': tmp_res})
                res += result
            # logger.debug("pipeline_info_extraction: numSentences=" + str(len(sentences)))
        # Now calculate and insert rating for product if available
        logger.debug("pipeline_info_extraction: review_count="+ str(review_count))
        if review_count:
            avg = review_score / review_count
            obj = {'attribute_id': self.rating_attribute_id,
                   'datatype': 'float', 'value_float': avg,
                   'value_node_id': None,
                   'master_product_id': product.id,
                   'source_id': self.source_id}

            res += [obj, ]
            # logger.debug("pipeline_info_extraction: review_count=" + str(review_count))

        # EXTRACT FROM PRODUCT NAME FOR THOSE THAT REQUIRE
        # logger.debug("pipeline_info_extraction: starts _extract_info for productname=" + product.name)
        result = self.extract_information(product.name, product.id,
                                             extract_name=True)
        # logger.debug("pipeline_info_extraction: done _extract_info for productname="+ product.name)
        res += result


        # EXTRACT FROM DESCRIPTION FOR THOSE THAT REQUIRE
        content = self.description_contents_data.get(product.id, [])
        tmp_res = []
        result = self.extract_from_descriptions(content, product.id, track_to=tmp_res)
        short_descs_data.append({
            'reviewer': None,
            'rating': None,
            'sentences': tmp_res})

        res += result

        # GET SHORT DESC
        summary, _ = summarize(short_descs_data)
        if summary:
            res.append(
                {
                    'attribute_id': self.short_desc_attribute_id,
                    'master_product_id': product.id,
                    'source_id': self.source_id,
                    'datatype': "text",
                    'value_text': summary
                }
            )


        # logger.debug("pipeline_info_extraction: done insert result into pipline_attribute_values for productname=" + product.name)

        # Update the master product with the extra (unknown) words found in name
        # print("** ",product.name," - ",extra_words)
        return res

    def process_all(self):
        begin_time = datetime.now()
        total = len(self.products)
        for pctr, product in enumerate(self.products):
            logger.debug("pipeline_info_extraction: " + str(pctr) + ' ' + product.name)
            # If we have a debug search string, continue if it's not in product name
            if self.debug_product_search_str and self.debug_product_search_str in product.name:
                continue

            self.description_contents_data = dict(
                get_description_contents_data(self.source_id,
                                              self.sequence_id))
            result = self.process_product(product)
            self.pav_bulk_inserter.insert_data(result)
            if not pctr % 100:
                end_time = datetime.now()
                delta = end_time - begin_time
                logger.debug(f'products: {pctr}/{total} | time {delta}')
                begin_time = datetime.now()

        self.pav_bulk_inserter.bulk_insert()
        return {"sequence_id": self.sequence_id}

    def extract_from_reviews(self, content, product_id, track_to=None):
        res: list
        sentences = split_to_sentences(content)
        for sctr, sentence in enumerate(sentences):
            res = []
            # logger.debug("pipeline_info_extraction: sentence=" + str(sctr) + ", lenSentence="+ str(len(sentence)))

            # Let's make sure there's a reasonable amount of content, not just initials or something
            if len(sentence) <= 5:
                continue
            # print('* {}'.format(sentence))
            result = self.extract_information(sentence, product_id, track_to=track_to)
            #
            # logger.debug("pipeline_info_extraction: sentence_idx="+ str(sctr) + ", done extract info")
            res += result
        return res
        # logger.debug("pipeline_info_extraction: sentence_idx=" + str(sctr) + ", done insert")

    def extract_from_descriptions(self, content, product_id, track_to=None):
        # description_contents = get_description_contents(product.id, source_id, sequence_id)

        # logger.debug("pipeline_info_extraction: done get description contents for productid=" + str(product.id))
        res = []
        if content:
            sentences = split_to_sentences(content)

            # logger.debug("pipeline_info_extraction: prod description contents, numSentences=" + str(len(sentences)))
            for sentence in sentences:
                # Let's make sure there's a reasonable amount of content, not just initials or something
                if len(sentence) <= 5:
                    continue
                # print('* {}'.format(sentence))
                result, _ = self.extract_information(sentence, product_id, track_to=track_to)
                # logger.debug("pipeline_info_extraction: done inserting sentence for prod description content")
                res += result
        return res

    def filter_temp_attributes(self, content, sentence, product_id,
                               domain_attributes):
        tmp_attributes = []
        for row in content:
            for pattern in row.extract_content_support['regex_patterns']:
                match = re.search(pattern, sentence, re.I)
                if match:
                    # Lookup id for attribute code
                    attr_id = [obj.id for obj in domain_attributes if
                               obj.code == row.code][0]
                    if row.datatype == 'boolean':
                        tmp_attributes.append({
                            'attribute_id': attr_id,
                            'code': row.code,
                            'master_product_id': product_id,
                            'source_id': self.source_id,
                            'datatype': 'boolean',
                            'value_boolean': True
                        })
                    elif row.datatype == 'float':
                        fl = float(match.group(1))
                        tmp_attributes.append({
                            'attribute_id': attr_id,
                            'code': row.code,
                            'master_product_id': product_id,
                            'source_id': self.source_id,
                            'datatype': 'float',
                            'value_float': fl
                        })
                    else:
                        raise NotImplementedError
        return tmp_attributes

    def generate_extract_result(self, attributes, domain_attributes,
                                product_id):
        result = []
        for obj in attributes:
            # Lookup id for attribute code
            attributes_same_code = [a.id for a in domain_attributes if
                                    a.code == obj['code']]
            if not attributes_same_code:
                continue
            attr_id = attributes_same_code[0]
            res = {'attribute_id': attr_id,
                   'master_product_id': product_id,
                   'source_id': self.source_id,
                   'value_node_id': None,
                   'original': obj.get('original', obj.get('value')),
                   'value_float': None,
                   'value': obj.get('value'),
                   }
            if 'node_id' in obj:
                res.update({'datatype': "node_id",
                            'value_node_id': obj['node_id'],
                            })
            elif 'value_float' in obj:
                res.update(
                    {
                        'datatype': "float",
                        'value_float': obj['value_float'],
                    }
                )

            elif 'value_boolean' in obj:
                res.update(
                    {
                        'datatype': "boolean",
                        'value_boolean': obj['value_boolean'],
                    }
                )
            else:
                raise TypeError

            result.append(res)
        return result

    def extract_information(self, sentence, product_id, extract_name=False, track_to=None):
        orig = sentence
        extra_words = []
        # logger.debug("pipeline_info_extraction: _extract_information sentence=" + str(len(sentence)))
        if extract_name:
            domain_attributes = self.domain_attributes_for_product_name_extract
            extract_content = self.extract_content_for_product_name
            domain_attribute_codes = self.domain_attribute_codes_for_product_name
        else:
            domain_attributes = self.domain_attributes_for_extract
            extract_content = self.extract_content
            domain_attribute_codes = self.domain_attribute_codes
        # Remove the stopwords before sending in sentence. This is to improve lookup performance
        sentence = ' '.join([word for word in sentence.lower().split() if
                             word not in STOPWORDS])

        # logger.debug("pipeline_info_extraction: _extract_information num_extract_content=" + str(len(extract_content)))
        # We now do the appropriate filtering of attributes before we call this function
        tmp_attributes = self.filter_temp_attributes(extract_content,
                                                     sentence,
                                                     product_id,
                                                     domain_attributes)

        attrs = domain_attribute_lookup(sentence)['attributes']
        tmp_attributes += attrs
        tmp_attributes = remove_duplicates(tmp_attributes)
        # We only care about the domain_attributes that were sent in... ignore other "bycatch" attributes that were extracted
        # tmp_attributes = [attr for attr in tmp_attributes if attr['code'] in [attr['code'] for attr in domain_attributes]]
        result = self.generate_extract_result(tmp_attributes,
                                              domain_attributes, product_id)
        if track_to is not None:
            attributes = []
            for r in result:
                attr = self.attrdict[r['attribute_id']]
                value_integer = r.get('value_integer')
                value_node_id = r.get('value_node_id')
                value_float = r.get('value_float')
                value_boolean = r.get('value_boolean')
                value_text = r.get('value_text')
                value = None
                if value_node_id:
                    if not value:
                        value = db.session.query(
                            DomainTaxonomyNode.id, DomainTaxonomyNode.name
                        ).filter_by(id=r['value_node_id']).first()[1]
                elif value_integer:
                    value = value_integer
                elif value_float:
                    value = value_float
                elif value_boolean:
                    value = value_boolean
                elif value_text:
                    value = value_text
                #for k in ['attribute_id',
                #          'master_product_id',
                #          'source_id']:
                #    r.pop(k)
                attributes.append({
                    'code': attr['code'],
                    'value': value,
                    'original': r['original'],
                })
            track_to.append({
                'text': orig,
                'attributes': attributes
            })
        # logger.debug("pipeline_info_extraction: _extract_information completes")
        return result

def get_domain_attributes_from_db(**kwargs):
    rows = db.session.query(DomainAttribute).filter_by(**kwargs)
    # extract and convert to list
    output = []
    for row in rows:
        obj = {
               'id': row.id,
               'code': row.code,
               'extract_content_support': row.extract_content_support,
               'datatype': row.datatype,
               'should_extract_values': row.should_extract_values,
               'should_extract_from_name': row.should_extract_from_name
               }
        output.append(obj)

    return output
