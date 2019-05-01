from distutils.util import strtobool
from os import getenv, environ
from pathlib import Path

VERSION = "2.22.9pm"

DEBUG = bool(
    strtobool(getenv('DEBUG', 'True')))  # Set DEBUG=False on production!

_environ = '\n'.join([f'{k}={v}' for k, v in environ.items()])
print(f"OS ENVIRONMENT:\n{_environ}")

BASE_PATH = Path(__file__).parents[1]
MODELS_PATH = BASE_PATH / 'models'
RESOURCES_PATH = BASE_PATH / 'resources'
SPACY_MODEL_PATH = BASE_PATH / 'models'
#PRELOAD_DATA = bool(int(getenv('PRELOAD_DATA', 1))) not working now
#
#   CORS settings (required by Flask)
#

CORS_HEADERS = ['Content-Type', 'Authorization']
CORS_RESOURCES = {"/api": {"origins": "*"}}

#
#   Cache settings (required by Flask)
#

CACHE_TYPE = getenv('CACHE_TYPE',
                    'redis')  # memcached, redis... https://pythonhosted.org/Flask-Caching/
CACHE_DIR = getenv('CACHE_DIR',
                   '/tmp/.nlu_cache')  # Directory to store cache. Used only for FileSystemCache
# (CACHE_TYPE=filesystem)
CACHE_THRESHOLD = int(getenv('CACHE_THRESHOLD', 1024))

CACHE_DEFAULT_TIMEOUT = 600  # in seconds

ATTRIBUTE_INTENTS = ['find-product', 'find-similar', 'find-different',
                     'confirm-property', 'confirm-good',
                     'get-property', 'define', 'compare', 'show-more',
                     'get-count', 'number']

NEGATOR_WORDS = ['not', 'no', 'non', 'none', 'hate', 'never', 'without',
                 'except']

COMMON_WORDS_PATH = BASE_PATH / 'data/stop_words.txt'

# SIMILAR_BREAK_WORDS = ['but', 'although', 'except']
# SIMILAR_WORDS = ['similar', 'similar_to', 'like']

DEFAULT_CATEGORY_ID = 1

# NER_MODEL_PATH = os.path.join(BASE_DIR, 'models')
OPERATOR_MODEL_PATH = MODELS_PATH / 'price_operator_model.pkl'

COUNT_VECT_MODEL_PATH = MODELS_PATH / 'count_vect.pkl'
DICT_VECT_MODEL_PATH = MODELS_PATH / 'dict_vect.pkl'
INTENT_CLF_MODEL_PATH = MODELS_PATH / 'sgd_clf.pkl'

# CLF1_MODEL_PATH = MODELS_PATH / 'triple_classifier_model/classifier1.pkl'
# CLF_NON_ATTRIBUTE_MODEL_PATH = MODELS_PATH / 'triple_classifier_model/classifier_non_attribute.pkl'
# CLF_ATTRIBUTE_MODEL_PATH = MODELS_PATH / 'triple_classifier_model/classifier_attribute.pkl'
CLF_UNIVERSAL_MODEL_PATH = MODELS_PATH / 'triple_classifier_model/classifier_universal_intent.pkl'

CLF_KEYWORDS_PATH = MODELS_PATH / 'triple_classifier_model/training_data_with_keywords.pkl'

LOOKUP_COMMON_VERBS = {'want', 'give', 'show', 'like', 'do', 'is', 'be',
                       'will', 'did', 'should',
                       'are', 'be', 'find', 'costs', 'need', 'does', 'have',
                       'tell', 'am',
                       'compare', 'define', 'show', 'display', 'has', 'see'}

LOOKUP_COMMON_PREPOSITIONS = {'to', 'from', 'over', 'around', 'with', 'under',
                              'without'}

LOOKUP_ALL_PREPOSITIONS = {'about', 'below', 'off', 'toward', 'above',
                           'beneath', 'for', 'on', 'under',
                           'across', 'besides', 'from', 'onto', 'after',
                           'between', 'in', 'out', 'until',
                           'against', 'up', 'along', 'but', 'inside', 'over',
                           'upon', 'among', 'by', 'past',
                           'around', 'concerning', 'regarding', 'with', 'at',
                           'despite', 'into', 'since',
                           'within', 'like', 'through', 'without', 'near',
                           'except', 'of', 'to'}

#  Training settings
#

PICKLE_FILE_PATH = "./models/triple_classifier_model/training_data_with_keywords.pkl"
# ATTRIBUTE_INTENTS = ['find-product', 'find-similar', 'find-different',
#                     'confirm-property', 'confirm-good', 'get-property', 'define', 'compare',
#                     'show-more', 'get-count']
# DB_NAME = 'legoly_v2'
# DB_USER = 'dbadmin'
# DB_PASSWORD = 'Char876Win'
# DB_HOST = '34.195.170.177'
# DB_PORT = 5432
# REQUEST_URL = 'http://127.0.0.1:5000/api/1/requests'
# CLASSIFIER1 = './models/triple_classifier_model/classifier1.pkl'
# CLASSIFIER_ATTRIBUTE = './models/triple_classifier_model/classifier_attribute.pkl'
# CLASSIFIER_NON_ATTRIBUTE = './models/triple_classifier_model/classifier_non_attribute.pkl'
# CLASSIFIER_UNIVERSAL_INTENT = './models/triple_classifier_model/classifier_universal_intent.pkl'
# VECTORIZER1 = './triple_classifier_model_search_str_keywords/vectorizer1.pkl'
# DVECTORIZER1 = './triple_classifier_model_search_str_keywords/dvectorizer1.pkl'
# VECTORIZER_ATTRIBUTE = './triple_classifier_model_search_str_keywords/vectorizer_attribute.pkl'
# VECTORIZER_NON_ATTRIBUTE = './triple_classifier_model_search_str_keywords/vectorizer_non_attribute.pkl'

NER_TRAINING_RATIO = 0.95
INTENT_TRAINING_RATIO = 0.95
ENTITY_NAME = 'PRICE_PHRASE'
OPERATOR_CLASSIFIER_PATH = MODELS_PATH / 'price_operator_model.pkl'

# context classifier model path
BASE_CONTEXT_MODEL_PATH = MODELS_PATH / 'context_classifier/'
CONTEXT_CLF_MODEL_PATH = BASE_CONTEXT_MODEL_PATH / 'context_classifier.pkl'
CONTEXT_CV_MODEL_PATH = BASE_CONTEXT_MODEL_PATH / 'context_count_vectorizer.pkl'
CONTEXT_DV_MODEL_PATH = BASE_CONTEXT_MODEL_PATH / 'context_dict_vectorizer.pkl'

# WORD MATCHING FACTORS
LOOKUP_MIN_FUZZY_WORD_LENGTH = 4  # word has to be n chars for us to fix
LOOKUP_FUZZY_THRESHOLD = 80  # higher numbers are safer
LOOKUP_NGRAM_BONUS = 0.50
LOOKUP_FUZZY_PENALTY = 0.05
LOOKUP_PERFECT_BONUS = 1.25
LOOKUP_BRAND_PENALTY = 0.30
LOOKUP_IDF_CUTOFF_FACTOR = 0.07  # % most common words

LOOKUP_MIN_IDF_CUTOFF = 5.5  # replace with calc'd
LOOKUP_HIGHER_IDF_CUTOFF = 8.0  # replace this hard-coded version with calc'd

# We probably want to automatically generate from non-semantic words in training data
# These are words that if they exist in query or entity, then we have to match exactly
LOOKUP_MATCH_WORDS = (
    'compare', 'difference', 'dollar', 'dollars', 'less', 'maybe', 'more',
    'one', 'popular', 'range', 'selection',
    'selections', 'similar', 'thing')

'''
LOOKUP_LOCAL_STOPWORDS = {'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 'your', 'yours',
                             'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', 'her', 'hers',
                             'herself', 'it', 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves',
                             'what', 'which', 'who', 'whom', 'this', 'that', 'these', 'those', 'am', 'is', 'are',
                             'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does',
                             'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until',
                             'while', 'of', 'at', 'by', 'for', 'with', 'about', 'between', 'into',
                             'through','before', 'after', 'above', 'below', 'to', 'from', 'up', 'down',
                             'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here',
                             'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more',
                             'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so',
                             'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', 'should', 'now',
                          'difference', 'different', 'same', 'similar', 'compare', 'comparison', 'better', 'price', 'range',
                          'less', 'more', 'between', 'something'
                          }
'''
# LOOKUP_LOCAL_STOPWORDS = set()

LOOKUP_COMMON_WORDS = {'a', 'and', 'the', 'an', 'du', 'del', 'do', 'da', 'le', 'la', 'i', 'co',
                       'company', 'inc', 'no', 'not', 'or'}
LOOKUP_COMMON_PENALTY = 0.90
LOOKUP_STOPWORDS = list(LOOKUP_COMMON_WORDS)


SCHEDULE_TIMEOUT_PIPELINE = int(getenv('SCHEDULE_TIMEOUT_PIPELINE', 0))
SCHEDULE_TIMEOUT_LOOKUP = int(getenv('SCHEDULE_TIMEOUT_LOOKUP', 0))

#
#   Startup settings
#
REFRESH_DICTS_ON_START = bool(int(getenv('REFRESH_DICTS_ON_START', 1)))
DICTONARY_UPDATE_TIMEOUT = int(getenv('DICTONARY_UPDATE_TIMEOUT', 60*15))
