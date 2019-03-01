import collections
from datetime import datetime
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer

from application.db_extension.dictionary_lookup import config
from application.logging import logger

from application.db_extension.dictionary_lookup.postgres_functions import (
    get_dict_items_from_sql)
from application.db_extension.dictionary_lookup.utils import (
    get_starting_chr_bigrams)


TOKEN_PATTERN = r'(?u)\b[\w\.]+\b'


def convert_to_dict_lookup(data,
                           existing_entries=None,
                           log_function=logger.info):
    from application.db_extension.dictionary_lookup.utils import cleanup_string, remove_stopwords

    if not existing_entries:
        existing_entries = set()

    def _convert(row):
        orig_str = row['text_value']
        row['original_text_value'] = orig_str
        # Use the value from postgres if it's there (should always be there)
        cleaned_string = row['text_value_processed'] if row.get('text_value_processed') else cleanup_string(
            row['text_value'])
        row['text_value'], _ = remove_stopwords(cleaned_string)
        if row['text_value'] == '':
            logger.debug("Add to nlp_ngrams: ", orig_str)
            row['text_value'] = orig_str
        # row['text_value'] = unidecode(row['text_value'])  # remove accents -> convert letters like å to a
        row['words'] = row['text_value'].split()
        row['word_count'] = len(row['words'])
        row['id'] = row['id']
        row['entity_id'] = row['entity_id']
        row['category_id'] = row['category_id']
        row['attribute_id'] = row['attribute_id']
        row['derived_definition'] = row.get('derived_definition', '')
        row['derived_guides'] = row.get('derived_guides', [])
        row['ancestor_node_length'] = row.get('ancestor_node_length', -1)
        return row

    log_function('got data from database, starting convert process...')
    fieldnames = ('id',
                  'category_id',
                  'attribute_id',
                  'entity_id',
                  'text_value',
                  # 'derived_expression',
                  # 'derived_definition',
                  # 'derived_guides',
                  # 'is_require_all_words',
                  'base_value',
                  # 'word_vector',
                  'attribute_code',
                  # 'text_value_processed',
                  # 'ancestor_node_length'
                  )
    result = []
    # remove already existing rows from the list:
    rows = [row for row in data if row.id not in existing_entries]
    log_function('{} rows to convert'.format(len(rows)))
    for i, row in enumerate(rows):
        if not i % 10000:
            log_function('{} rows converted'.format(i))
        result.append(_convert(dict(zip(fieldnames, row))))
    return result


def process_dictionary(entities, log_function=logger.info):
    from application.db_extension.dictionary_lookup.file_service import (
        lookup_entities,
        lookup_inverted_index,
        lookup_word_idf,
        lookup_ordered_entities,
        lookup_entities_text_id_dict,
        lookup_idf_statistics)
    entities_text = [entity['text_value'] for entity in entities]
    vectorizer = TfidfVectorizer(ngram_range=(1, 1), norm=None, use_idf=True,
                                 sublinear_tf=True, token_pattern=TOKEN_PATTERN)
    _ = vectorizer.fit_transform(entities_text)
    idf = vectorizer.idf_
    idf_dict = dict(zip(vectorizer.get_feature_names(), idf))

    # Get a cutoff for common words that we will use to determine whether it will satisfy the ngram intersection
    # for entities, or in the case of very common ngrams we will want a second ngram to match as well.
    # This is to deal with problems like 10,000 instances of 'chateau'. It also helps us avoid poor brand matches
    cutoff_factor = config.LOOKUP_IDF_CUTOFF_FACTOR  # most common percentile of words
    cutoff_idf = np.percentile(idf, cutoff_factor)

    # Add max theoretical score = idf * BIGRAM_BONUS * PERFECT_BONUS
    # Also determine whether entity requires non-common ngrams to match later
    # Then sort and save dict with entity_id as key
    for entity in entities:

        words = entity['words']
        max_score = 0
        insufficient_ngrams = []
        for word in words:
            if not idf_dict.get(word, None):
                log_function(
                    f"Could not get word: {word} for entity {entity['text_value']}")
                continue
            max_score += idf_dict[word]
            ngram = get_starting_chr_bigrams([word])[0]
            if idf_dict[word] < cutoff_idf:
                insufficient_ngrams.append(ngram)

        # Make sure we didn't exclude everything!
        if len(insufficient_ngrams) == len(words):
            insufficient_ngrams = []
        # convert to set for fast lookup
        entity['insufficient_ngrams'] = set(insufficient_ngrams)

        # Set max theoretical idf (we aren't currently using this - used maybe for early exit)
        entity['max_idf'] = max_score * config.LOOKUP_PERFECT_BONUS
        entity['max_idf'] *= (1 + len(words) *
                              config.LOOKUP_NGRAM_BONUS) if len(words) > 1 else 1

    sorted_entities = sorted(
        entities, key=lambda k: k['max_idf'], reverse=True)

    ordered_entities_dict = collections.OrderedDict()
    entities_text_id_dict = {}
    for entity in sorted_entities:
        ordered_entities_dict[entity['id']] = entity
        found_id = entities_text_id_dict.get(entity['text_value'], None)
        if not found_id:
            entities_text_id_dict[entity['text_value']] = entity['id']
        else:
            # We already have an entity stored under text value. So we have an ambiguous
            #  situation where multiple attributes have same text_value... so we should store
            #  a list of entity ids, not just a single id. Also, don't bother adding another one if its
            #  the same attribute code
            first_id = found_id if isinstance(found_id, int) else found_id[0]
            if entity['attribute_code'] == ordered_entities_dict[first_id]['attribute_code']:
                continue
            elif isinstance(found_id, int):
                entities_text_id_dict[entity['text_value']] = [
                    found_id, entity['id']]
            else:  # is set
                entities_text_id_dict[entity['text_value']].append(
                    entity['id'])

    # Save dictionaries
    log_function('saving dictionaries')
    lookup_word_idf.dump(idf_dict)
    lookup_ordered_entities.dump(ordered_entities_dict)
    lookup_entities_text_id_dict.dump(entities_text_id_dict)
    lookup_idf_statistics.dump(cutoff_idf)

    return


# INVERTED INDEX FUNCTION
def create_ngrams(entities, existing_index):
    from application.db_extension.dictionary_lookup.file_service import lookup_inverted_index
    inverted_index = existing_index
    for row in entities:
        chr_ngrams = get_starting_chr_bigrams(row['words'])
        for i, chr_grm in enumerate(chr_ngrams):
            if chr_grm not in inverted_index:
                inverted_index[chr_grm] = []
            inverted_index[chr_grm].append(row['id'])
    lookup_inverted_index.dump(inverted_index)
    return inverted_index


def get_avg_total_vector(tokens: list) -> list:
    avg_total_vector = np.zeros(300, dtype='f')
    num = 0
    # todo refactor
    for token in tokens:
        if token['vector']:
            avg_total_vector += token['vector']
            num += 1
    return avg_total_vector / num if num else None


def update_dictionary_lookup_data(log_function=logger.info):
    from application.db_extension.dictionary_lookup.file_service import lookup_entities
    log_function('starting dictionary lookup data update')
    start_time = datetime.now()
    log_function('getting existing index')
    existing_entries = set()
    existing_index = {}
    log_function('getting entities')
    data = get_dict_items_from_sql()
    entities = [e for e in
                convert_to_dict_lookup(data,
                                       existing_entries=existing_entries,
                                       log_function=log_function)
                if e['text_value']
                ]
    lookup_entities.dump(entities)
    log_function('creating ngram index')
    create_ngrams(entities, existing_index)
    log_function('processing dictionary')
    process_dictionary(entities, log_function=log_function)
    log_function('finished dictionary update in {}'.format(
        datetime.now() - start_time))
