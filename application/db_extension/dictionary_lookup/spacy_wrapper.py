import json
import hashlib
import base64
import requests
from application.logging import logger

from application.caching import cache

TIMEOUT = 60 * 60 * 24 * 7

_session = requests.Session()

'''
@cache.memoize(timeout=TIMEOUT)
def spacy_process_words(words: list):
    response = requests.post(SPACY_HOST + '/process_words', json={'words': words})
    data = json.loads(response.content.decode('utf8'))
    result = data['result']
    return result
'''


def get_spacy_result(response):
    if response.status_code == 200:
        data = json.loads(response.content.decode('utf8'))
    else:
        raise IOError()
    return data['result']


@cache.memoize(timeout=TIMEOUT)
def parse_string_via_spacy(sentence):
    response = requests.post(SPACY_HOST + '/parse',
                             json={'sentence': sentence})
    result = get_spacy_result(response)
    return result['tokenized_words'], result['pos_tags'], result['lemmatized_words']


@cache.memoize(timeout=TIMEOUT)
def get_pos_for_word(sentence: str) -> str:
    response = requests.post(SPACY_HOST + '/pos', json={'sentence': sentence})
    return get_spacy_result(response)


@cache.memoize(timeout=TIMEOUT)
def get_lemma_for_word(sentence):
    response = requests.post(SPACY_HOST + '/lemma',
                             json={'sentence': sentence})
    return get_spacy_result(response)


class LemmaCache:
    def __init__(self):
        self._data = {}

    @staticmethod
    def make_hash(key):
        # raw = '|'.join(value)
        raw = key
        cache_key = hashlib.md5()
        cache_key.update(raw.encode('utf-8'))
        cache_key = base64.b64encode(cache_key.digest())[:16]
        cache_key = cache_key.decode('utf-8')
        return cache_key

    def add(self, key, value):
        _hash = self.make_hash(key)
        self._data[_hash] = value

    def get(self, value):
        _hash = self.make_hash(value)
        result = self._data[_hash]
        return result


lemma_cache = LemmaCache()


@cache.memoize(timeout=TIMEOUT)
def remove_stopwords(sentence):
    response = requests.post(
        SPACY_HOST + '/remove_stopwords', json={'sentence': sentence})
    return get_spacy_result(response)


@cache.memoize(timeout=TIMEOUT)
def get_lemmas(l: list):
    response = requests.post(SPACY_HOST + '/lemmas', json={'words': l})
    if response.status_code != 200:
        logger.warning('trying to fallback to old spacyapp version API')
        data = [get_lemma_for_word(w) for w in l]
        return data
    else:
        return get_spacy_result(response)


@cache.memoize(timeout=TIMEOUT)
def nlp_list(sentences: list) -> list:
    try:
        response = requests.post(
            SPACY_HOST + '/nlp', json={'sentences': sentences})
        return get_spacy_result(response)
    except IOError as e:
        # try to fallback
        return [fallback_nlp(_s) for _s in sentences]


def nlp(sentence: str) -> list:
    """
    Shortcut for backward compatibility and when you don't need to process a list of sentences
    :param sentence:
    :return:
    """
    result = nlp_list([sentence, ])
    return result[0]


def fallback_nlp(sentence: str) -> list:
    response = requests.post(SPACY_HOST + '/nlp', json={'sentence': sentence})
    print("UPDATE SPACY")
    return get_spacy_result(response)


@cache.memoize(timeout=TIMEOUT)
def extract_number_entities(sentence):
    response = requests.post(
        SPACY_HOST + '/number_entities', json={'sentence': sentence})
    return get_spacy_result(response)
