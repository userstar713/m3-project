#!/usr/bin/env python
# -*- coding: utf-8 -*-
import random
from random import choice
import re
from collections import defaultdict, namedtuple

from application.db_extension.routines import get_default_category_id
from application.db_repo.models.domain_dictionary import DomainDictionary
from application.caching import cache
from application.db_extension.models import db
from application.logging import logger

DEFAULT_CATEGORY_ID = get_default_category_id()
all_att_names = []


# @cache.memoize() disable caching now due to issues with attribute_lookup
def cached_execute(query, arguments):
    # print('SQL: ', query, arguments)
    return list(db.engine.execute(query, *arguments))


def fetchall(query, *args):
    return cached_execute(query, args)


def filter_tsquery(s):
    return s.replace("'", " ").replace("!", '').replace("(", '').replace(")",
                                                                         '')


@cache.memoize()
def get_nlp_synonyms():
    rows = fetchall(
        'SELECT * from nlp_synonyms ORDER BY char_length(source_text) DESC;')

    # Precompile the regex for peformance
    out_rows = []
    for row in rows:
        r = dict(row)
        pattern = r'^{}\b' if r['must_be_at_beginning'] else r'\b{}\b' if r[
            'is_whole_word'] else r'{}'
        r['rg'] = pattern.format(r['source_text'])
        out_rows.append(r)
    return out_rows


@cache.memoize()
def get_process_product_attributes():
    rows = fetchall(
        'SELECT code from domain_attributes WHERE is_process_product=TRUE;')
    return [row[0] for row in rows]


# We should add category_id to the get_nlp_ngrams() function to avoid unnecessary substitutions from other categories
@cache.memoize(timeout=10 * 60)
def get_nlp_ngrams():
    logger.info('get_nlp_ngrams')
    rows = fetchall(
        "SELECT text_value_processed target_text, replace(text_value_processed,'_',' ') source_text "
        "FROM domain_dictionary WHERE is_require_ngram=TRUE "
        "ORDER BY char_length(text_value_processed) DESC")
    return {row['target_text']: row['source_text'] for row in rows}


@cache.memoize(timeout=200)
def get_nlp_ngrams_regex():
    ngrams = get_nlp_ngrams()
    regex = re.compile("(%s)" % "|".join(map(re.escape, ngrams.keys())))
    return ngrams, regex


@cache.memoize()
def get_attr_codes_by_priority(category_id):
    rows = fetchall(
        'SELECT code from domain_attributes WHERE category_id=%s ORDER BY priority;',
        (category_id,))
    return [row[0] for row in rows]


UnitsAndCategoryNames = namedtuple('UnitsAndCategoryNames',
                                   ['units',
                                    'categories',
                                    'trigger_words',
                                    'trigger_response',
                                    'trigger_language_words',
                                    'trigger_language_response'
                                    ]
                                   )


@cache.memoize()
def get_units_and_category_names(category_id=DEFAULT_CATEGORY_ID):
    q = '''SELECT NULL, 
                   name_singular, 
                   name_plural,
                   trigger_words, 
                   trigger_words_response, 
                   trigger_language_words, 
                   trigger_language_response  
           FROM domain_categories 
           WHERE id=%s 
           ORDER BY char_length(name_singular) DESC;'''
    rows = fetchall(q, (category_id,))
    row = rows[0]
    units = []
    # for i in range(len(row[0])):
    #     units.append(row[0][i]['singular'])
    categories = row[1:3]
    trigger_words = set(row[3])
    trigger_response = row[4]
    trigger_language_words = set(row[5])
    trigger_language_response = row[6]
    return UnitsAndCategoryNames(units,
                                 categories,
                                 trigger_words,
                                 trigger_response,
                                 trigger_language_words,
                                 trigger_language_response)


@cache.memoize()
def lookup_master_products(source_id, category_id, brand_node_id, extra_words):
    query_str = " | ".join(extra_words)  # "bob | vineyard"

    q = '''SELECT ts_rank(non_attribute_words_vector, keywords, 1),
                  mp.source_id,
                  mp.id master_product_id, 
                  mp.name,
                  mp.non_attribute_words,
                  mp.non_attribute_words_vector,
                  mp.brand_node_id
           FROM master_products mp, to_tsquery('english', %s) keywords
           WHERE brand_node_id=%s 
             AND mp.category_id=%s 
             AND mp.source_id>-1
             AND non_attribute_words_vector @@ keywords
           ORDER BY source_id=%s DESC, ts_rank(non_attribute_words_vector, keywords, 1) DESC
    LIMIT 50'''

    rows = fetchall(q, (query_str, brand_node_id, category_id, source_id))
    return [dict(row) for row in rows]


@cache.memoize()
def get_brands_for_source_and_category(source_id, category_id):
    # We will use this to determine whether a brand is being sold by the source
    if not source_id or not category_id:
        return []

    # Get list of node ids for brand for max sequence for source
    q = ''' SELECT pav.value_node_id node_id FROM pipeline_attribute_values pav, domain_attributes da 
            WHERE sequence_id = (select max(id) from pipeline_sequence WHERE id=pav.sequence_id) AND 
            source_id=%s AND category_id=%s AND pav.attribute_id=da.id AND da.code='brand';'''

    rows = fetchall(q, (source_id, category_id))
    return set([row[0] for row in rows])


@cache.memoize()
def has_intent_response(intent):
    rows = fetchall(
        'SELECT has_source_response FROM nlp_intents WHERE intent=%s;',
        (intent,))
    try:
        return rows[0][0]
    except IndexError:
        return False


@cache.memoize()
def get_intent_default_response(intent):
    rows = fetchall(
        'SELECT default_response FROM nlp_intents WHERE intent=%s;', (intent,))
    try:
        return choice(rows[0][0])
    except IndexError:
        return None


@cache.memoize()
def get_nlp_sub_intents():
    rows = fetchall(''' SELECT subintent, source_text, must_be_at_beginning
                        FROM nlp_subintents order by char_length(source_text) DESC;''')
    subintents = []
    for row in rows:
        subintents.append({
            'subintent': row[0],
            'source_text': row[1],
            'must_be_at_beginning': bool(row[2])
        })

    return subintents
    #   return [{'subintent':"want", 'source_text':"i want", 'must_be_at_beginning':True},
    # {'subintent':"define", 'source_text':"define", 'must_be_at_beginning':False},
    # {'subintent':"question-is", 'source_text':"is", 'must_be_at_beginning':True},
    # {'subintent':"compare", 'source_text':"difference", 'must_be_at_beginning':False},
    # {'subintent':"compare", 'source_text':"compare", 'must_be_at_beginning':False},
    # {'subintent':"question-does", 'source_text':"does", 'must_be_at_beginning':True},
    # {'subintent':"similar", 'source_text':"similar to", 'must_be_at_beginning':False},
    # {'subintent':"similar", 'source_text':"like", 'must_be_at_beginning':False}]


@cache.memoize()
def get_high_low_root_words(category_id=DEFAULT_CATEGORY_ID):
    q = ''' SELECT da.code as attribute, das.high_root_words, das.low_root_words
            FROM domain_attribute_support das, domain_attributes da
            WHERE da.category_id=%s AND das.attribute_id=da.id;'''
    rows = fetchall(q, (category_id,))
    root_words = {}
    for row in rows:
        cat = row[0]
        root_words[cat] = {}
        for word in row[1]:
            root_words[cat][word] = 'high'
        for word in row[2]:
            root_words[cat][word] = 'low'

    return root_words


@cache.memoize()
def get_attribute_predicates(category_id=DEFAULT_CATEGORY_ID):
    q = ''' SELECT da.code as attribute, dar.predicate_expression, 
    dar.predicate_for_disambiguation, dar.predicate_preposition_expression, da.priority
            FROM domain_attribute_relationships dar, domain_attributes da
            WHERE da.category_id=%s AND dar.object_attribute_id=da.id AND da.has_taxonomy=true
                AND da.taxonomy_is_scored is false ORDER BY da.priority;'''
    return fetchall(q, (category_id,))


@cache.memoize()
def get_attributes(category_id=DEFAULT_CATEGORY_ID):
    q = ''' SELECT da.name,
                   da.aliases, 
                   da.code,
                   da.is_intrinsic, 
                   da.datatype,
                   da.part_of_speech, 
                   da.id, 
                   da.taxonomy_is_scored, 
                   da.is_check_part_of_speech, 
                   da.is_after_brand,
                   da.word_vector -> 'single_avg_vector' word_vector
            FROM domain_attributes da WHERE category_id=%s;'''
    rows = fetchall(q, (category_id,))
    output = []
    all_att_names = []
    for row in rows:
        output.append({
            'name': row.name,
            'alias': row.aliases,
            'code': row.code,
            'is_intrinsic': row.is_intrinsic,
            'datatype': row.datatype,
            'pos': row.part_of_speech,
            'id': row.id,
            'taxonomy_is_scored': row.taxonomy_is_scored,
            'is_check_part_of_speech': row.is_check_part_of_speech,
            'is_after_brand': row.is_after_brand,
            'word_vector': row.word_vector
        })

        all_att_names.append(row.name)
        all_att_names.extend(row.aliases)

    all_att_names = sorted(all_att_names, key=len, reverse=True)
    return output, all_att_names


def get_dict_items_from_sql(category_id=DEFAULT_CATEGORY_ID):
    """
    Return raw SQL data for future processing in convert_to_dict_lookup
    :param category_id:
    :return:
    """
    q = """
        SELECT dd.id,
           dd.category_id,
           dd.attribute_id,
           dd.entity_id entity_id,
           dd.text_value,
           dd.source_entity_content->>'base' base_value,
           da.code attribute_code
        FROM domain_dictionary dd, domain_taxonomy_nodes dtn, domain_attributes da
        WHERE dd.entity_id = dtn.id
        -- AND dd.entity_type = 'node'
        AND dtn.attribute_id = da.id
        AND da.category_id=%s
    """

    # Remove brand restrictions for now
    '''  AND CASE 
          WHEN da.code='brand' 
          THEN dtn.id IN (SELECT distinct(value_node_id) FROM pipeline_attribute_values) ELSE 1=1 
        END;'''

    rows = fetchall(q, (category_id,))
    return rows


def fetch_entities_without_vectors(category_id=DEFAULT_CATEGORY_ID):
    q = """
        SELECT dd.id, dd.text_value 
        FROM domain_dictionary dd, domain_attributes da
        WHERE dd.word_vector='[]'
        AND dd.entity_type='node'
        AND da.code <> 'brand' 
        AND da.id=dd.attribute_id
        AND da.category_id=%s
    """
    rows = fetchall(q, (category_id,))
    return rows


def update_entity_vector(vectors):
    for k, v in vectors.items():
        db.session.execute(
            DomainDictionary.__table__.update()
                .values(word_vector=v)
                .where(
                    DomainDictionary.__table__.c.id == k)
        )
    # commit changes
    db.session.commit()


@cache.memoize()
def lookup_intent_response(intent, source_id):
    rows = fetchall(
        'SELECT response FROM nlp_intent_response WHERE intent=%s AND source_id=%s;',
        (intent, source_id))
    if rows:
        return random.choice(rows[0][0])
    else:
        return None


@cache.memoize()
def pipeline_attribute_lookup(sentence, category_id=DEFAULT_CATEGORY_ID):
    # Remove potentially problematic chars
    sentence = re.sub('[^A-Za-z0-9$]+', ' ', sentence).lstrip()

    q = "SELECT * from public.attribute_lookup2 (%s, %s, NULL, 'exclude', TRUE, FALSE);"

    # Note 'exclude' because we don't extract brands and False because we don't want to constrain to current store (which we are populating!)
    rows = list(fetchall(q, (category_id, filter_tsquery(sentence))))
    attributes = []
    for row in rows:
        atts = row[0].get('attributes', None)
        # atts = row.attribute_lookup['attributes']
        if atts:
            attributes.extend(atts)
    return attributes


# Modify this so we SELECT value_text from pipeline_attribute_values where master_product_id=123 AND
def get_description_contents(master_product_id, source_id, sequence_id):
    q = "SELECT value_text from pipeline_attribute_values pav, domain_attributes da " \
        "WHERE pav.master_product_id=%s AND pav.source_id=%s " \
        "AND pav.sequence_id=%s AND pav.attribute_id=da.id " \
        "AND da.code = 'description' "

    # execute the query and return the rows (will be 0 or 1)
    rows = fetchall(q, (master_product_id, source_id, sequence_id))
    return rows


def get_description_contents_data(source_id, sequence_id):
    q = "SELECT master_product_id,value_text from pipeline_attribute_values pav, domain_attributes da " \
        "WHERE pav.source_id=%s " \
        "AND pav.sequence_id=%s AND pav.attribute_id=da.id " \
        "AND da.code = 'description' "
    rows = fetchall(q, (source_id, sequence_id))
    return [(row.master_product_id, row.value_text) for row in rows]


def fuzzy_lookup_attributes(s, category_id=DEFAULT_CATEGORY_ID):
    q = "select dd.text_value, levenshtein(lower(dd.text_value), %s, 1, 1, 1)  + " \
        "levenshtein(soundex(dd.text_value),soundex(%s)) as distance FROM domain_dictionary dd " \
        "WHERE dd.text_value ILIKE %s AND " \
        "levenshtein(lower(dd.text_value), %s, 1, 1, 1) + levenshtein(soundex(dd.text_value),soundex(%s)) < 2 " \
        "AND dd.category_id = %s AND " \
        "EXISTS (SELECT value_node_id FROM pipeline_attribute_values pav WHERE pav.value_node_id = dd.entity_id) " \
        "ORDER BY levenshtein(lower(dd.text_value), %s, 1, 1, 1) + " \
        "levenshtein(soundex(dd.text_value),soundex(%s)) " \
        "LIMIT 10"

    if s:
        rows = fetchall(q, (s, s, s[0] + '%', s, s, category_id, s, s))
        if rows:
            return rows[0][0]
    return None


def get_intent_training_data():
    print("Getting intent training data!")
    rows = list(fetchall(
        'SELECT intent_code, text, sample_sentence FROM nlp_intent_training'))
    return rows


def get_numeric_training_data():
    rows = fetchall('SELECT * from nlp_numeric_training')
    numeric_data = []
    # skip first row which contains column headers
    for row in rows[1:]:
        # skip first column containing ID
        numeric_data.append(row[1:])

    return numeric_data


def get_context_data_from_db():
    """
        Fetch context data from db - required for context classifier training
    """
    query = 'SELECT classification, ' \
            'sentence, ' \
            'intersects, ' \
            'context' \
            ' FROM nlp_context_training'
    rows = list(fetchall(query))
    db_data = defaultdict(list)
    for classification, sentence, intersects, context in rows:
        classification = classification.strip()
        if classification:
            sentence = sentence.strip()
            intersects = intersects.strip()
            context = context.strip()
            result = (sentence, intersects, context)
            db_data[classification].append(result)
    return db_data


# To validate the pipeline execution
def validate_pipeline_run(db_session,
                          category_id, source_id, sequence_id):
    logger.info(f"running postgresql public.validate_pipeline_run"
                "category_id={category_id}"
                "source_id={source_id}"
                "sequence_id={sequence_id}")
    q = """SELECT check_type, status 
           FROM public.validate_pipeline_run(:category_id, 
                                             :source_id, 
                                             :sequence_id)"""
    rows = db_session.execute(q,
                              {'category_id': category_id,
                               'source_id': source_id,
                               'sequence_id': sequence_id})
    rows = list(rows)
    if rows is None or len(rows) < 2:
        return False
    for row in rows:
        if not row['status']:
            logger.info('validate_pipeline_run: status is false for ' + row[
                'check_type'])
            return False
        logger.info(
            'validate_pipeline_run: status is true for ' + row['check_type'])
    return True


def get_master_product_id(agent_id, source_prod_id):
    query = "SELECT master_product_id from public.source_attribute_values where agent_id =%s and source_product_id = %s LIMIT 1"
    rows = list(fetchall(query, (agent_id, source_prod_id)))
    if rows is None or len(rows) < 1:
        return None
    return rows[0]['master_product_id']

# To validate the pipeline execution


def update_price(agent_id, sequence_id, source_product_id, price, qoh):
    result = None
    try:
        # TODO - Need to use COMMIT; here in order for it to commit to the session
        query = 'BEGIN;SELECT * FROM public.update_price(%s, %s, %s,%s, %s);COMMIT;SELECT 0;'
        rows = list(
            fetchall(query, (agent_id, sequence_id, source_product_id, price, qoh)))
        #logger.debug('row: {}'.format(rows[0]))
        result = rows[0]
    except BaseException as e:
        logger.error('Got exception {}'.format(e))
    return result
