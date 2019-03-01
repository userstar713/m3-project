#
#   Working with files
#

from .models import (PickleDataFile,
                     JoblibDataFile,
                     PickleCacheDataFile,
                     JoblibCacheDataFile)
from application.db_extension.dictionary_lookup.config import (
    CLF_KEYWORDS_PATH,
    OPERATOR_MODEL_PATH,
    CONTEXT_DV_MODEL_PATH,
    CONTEXT_CV_MODEL_PATH,
    CONTEXT_CLF_MODEL_PATH,
    CLF_UNIVERSAL_MODEL_PATH,
    LOOKUP_ENTITIES_FILENAME,
    LOOKUP_INVERTED_INDEX_FILENAME,
    LOOKUP_WORD_IDF_DICT_FILENAME,
    LOOKUP_ORDERED_ENTITIES_DICT_FILENAME,
    LOOKUP_ENTITIES_TEXT_ID_DICT_FILENAME,
    LOOKUP_IDF_STATISTICS_FILENAME,
)

clf_sentence_keywords = PickleDataFile(filename=CLF_KEYWORDS_PATH)
#
operator_clf = JoblibDataFile(filename=OPERATOR_MODEL_PATH)
clf_universal_attribute = JoblibDataFile(filename=CLF_UNIVERSAL_MODEL_PATH)
#
context_clf = JoblibDataFile(filename=CONTEXT_CLF_MODEL_PATH)
context_dv = JoblibDataFile(filename=CONTEXT_DV_MODEL_PATH)
context_cv = JoblibDataFile(filename=CONTEXT_CV_MODEL_PATH)
# lookup_entities = PickleDataFile(filename=LOOKUP_ENTITIES_FILENAME)
# lookup_inverted_index = PickleDataFile(filename=LOOKUP_INVERTED_INDEX_FILENAME)
# lookup_word_idf = PickleDataFile(filename=LOOKUP_WORD_IDF_DICT_FILENAME)
# lookup_ordered_entities = PickleDataFile(filename=LOOKUP_ORDERED_ENTITIES_DICT_FILENAME)
# lookup_entities_text_id_dict = PickleDataFile(filename=LOOKUP_ENTITIES_TEXT_ID_DICT_FILENAME)
# lookup_idf_statistics = PickleDataFile(filename=LOOKUP_IDF_STATISTICS_FILENAME)

lookup_entities = PickleCacheDataFile(filename=LOOKUP_ENTITIES_FILENAME)
lookup_inverted_index = PickleCacheDataFile(
    filename=LOOKUP_INVERTED_INDEX_FILENAME)
lookup_word_idf = PickleCacheDataFile(filename=LOOKUP_WORD_IDF_DICT_FILENAME)
lookup_ordered_entities = PickleCacheDataFile(
    filename=LOOKUP_ORDERED_ENTITIES_DICT_FILENAME)
lookup_entities_text_id_dict = PickleCacheDataFile(
    filename=LOOKUP_ENTITIES_TEXT_ID_DICT_FILENAME)
lookup_idf_statistics = PickleCacheDataFile(
    filename=LOOKUP_IDF_STATISTICS_FILENAME)
