from datetime import datetime


from application.db_extension.dictionary_lookup import config
from application.logging import logger

from application.db_extension.dictionary_lookup.postgres_functions import (
    lookup_master_products)
from application.db_extension.dictionary_lookup.process_dictionary import (
    get_dict_items_from_sql,
    convert_to_dict_lookup,
    create_ngrams,
    process_dictionary)
from application.db_extension.dictionary_lookup.utils import (
    remove_stopwords,
    get_starting_chr_bigrams,
)
from fuzzywuzzy import fuzz

MIN_SCORE = 0  # minimum score to accept entity
UNMATCHABLE = '*********'  # unmatchable token


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(
                Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class DictionaryLookupClass(metaclass=Singleton):
    """
    Implements dictionary lookup logic

    """

    def __repr__(self):
        return self.__class__.__name__

    def __init__(self):
        """
        Load dictionary data
        """
        self.word_idf_dict = None
        self.ngram_inverted_index = None
        self.entities_text_id_dict = None
        self.entities_dict = None
        self._updated = False
        self._files = []
        self.word_lemma_dictionary_for_query = {}
        self.last_time_dictionary_updated = None

    def get_dict_entity_for_str(self, s):
        return self.entities_text_id_dict.get(s, None)

    # If we have more than one id, find the one with the first code in ordered_code list
    def disambiguate_multiple_entities(self, found, ordered_codes):

        if not found or len(found) == 0:
            return None

        for code in ordered_codes:
            for i in found:
                if self.entities_dict[i]['attribute_code'] == code:
                    return i
        # Code should always be found if code list is passed in, but if empty then just return the first id
        return found[0]

    @staticmethod
    def get_run_length(matched_words):
        max_run = 1
        for index, word in enumerate(matched_words[:-1]):  #
            query_indx = matched_words[index]['query_indx']
            cand_indx = matched_words[index]['cand_indx']
            next_query_indx = matched_words[index + 1]['query_indx']
            next_cand_indx = matched_words[index + 1]['cand_indx']

            if (next_query_indx - query_indx == 1) and (
                    next_cand_indx - cand_indx == 1):
                max_run += 1
            else:
                max_run = 1

        return max_run

    def get_bigram_lists(self, bigrams, attr_codes):
        all_entities = []
        for chr_ngram in bigrams:
            if chr_ngram in self.ngram_inverted_index:
                ids_to_add = [i for i in self.ngram_inverted_index[chr_ngram]
                              if chr_ngram not in self.entities_dict[i][
                                  'insufficient_ngrams']]
                all_entities.extend(ids_to_add)

        # If the query contains only unknown ngrams (e.g., 'rred' will be 'rre' which doesn't match anything)
        # then we will include all entities that have same first letter as first ngram. This will be slow but
        # better than missing word.
        # DO WE NEED TO MODIFY THIS TO LOOK AT ALL INPUT BIGRAMS INSTEAD OF JUST FIRST ONE?
        if len(all_entities) == 0 and len(bigrams) > 0:
            for i, (k, v) in enumerate(self.entities_dict.items()):
                if bigrams[0][0] in [word[0] for word in
                                     v['text_value'].split()]:
                    all_entities.append(k)

        # Constrain attributes to optional constrained list in attr_codes
        if attr_codes and len(attr_codes) > 0:
            all_entities = [entity for entity in all_entities if
                            self.entities_dict[entity][
                                'attribute_code'] in attr_codes]

        return set(all_entities)

    @staticmethod
    def product_lookup2(brand_node_id, source_id, category_id, orig_sentence):
        product_objs = lookup_master_products(source_id, category_id,
                                              brand_node_id,
                                              orig_sentence.split())
        prod_words = []
        for prod in product_objs:
            # Convert null to empty string for later processing
            prod['non_attribute_words'] = prod['non_attribute_words'] if prod[
                'non_attribute_words'] else ''
            prod_words.extend(prod['non_attribute_words'].split())

        # remove any products that don't have any overlapping non_attribute_words
        products = [product for product in product_objs if
                    len(set(
                        product['non_attribute_words'].split()).intersection(
                        set(orig_sentence.split()))) > 0]
        return products

    def format_as_predicate_syntax(self, entities):
        attribs = []
        for entity in entities:
            value = self.entities_dict[entity['id']]['base_value']
            value = entity['text'] if not value else value
            obj = {'code': entity['attribute_code'],
                   'value': value,
                   'original': entity['text'],
                   'node_id': entity['entity_id'],
                   'start': entity['start'],
                   'end': entity['end'],
                   'derived_definition': self.entities_dict[entity['id']].get(
                       'derived_definition', ""),
                   'derived_guides': self.entities_dict[entity['id']].get(
                       'derived_guides', []),
                   'ancestor_node_length': self.entities_dict[entity['id']].get('ancestor_node_length',-2),
                   'max_idf': entity['max_idf']
                   }

            attribs.append(obj)
        return attribs

    def get_matched_unmatched_words(self, words_in_query, words_in_entity,
                                    is_allow_fuzzy, is_brand):
        matched_words = []
        unmatched_words = []
        indices_matched = []
        lemmas = self.word_lemma_dictionary_for_query
        for ei, ew in enumerate(words_in_entity):
            # if ew not in self.word_idf_dict: # skip if not in dictionary
            #    continue
            match_found = False
            for qi, qw in enumerate(words_in_query):
                # if exact match, add as is
                if ew == qw and qi not in indices_matched:
                    idf = self.word_idf_dict.get(ew, 0)
                    matched_words.append(
                        {'query_indx': qi, 'cand_indx': ei, 'score': 1.0,
                         'token': ew, 'query_token': qw, 'idf': idf})
                    match_found = True
                    indices_matched.append(qi)
                    break
                # Do fuzzy check if not exact (must have same first char)
                elif qw[0] == ew[0] and is_allow_fuzzy:

                    if len(qw) >= config.LOOKUP_MIN_FUZZY_WORD_LENGTH and len(
                            ew) >= config.LOOKUP_MIN_FUZZY_WORD_LENGTH:
                        fuzzy_score = fuzz.ratio(qw, ew)
                    # special logic for very short words - only support an ending 's' difference (e.g., cab vs cabs and not brand)
                    elif len(
                            qw) == config.LOOKUP_MIN_FUZZY_WORD_LENGTH - 1 or len(
                        ew) == config.LOOKUP_MIN_FUZZY_WORD_LENGTH - 1:

                        # if is_brand and (qw + 's' == ew or ew + 's' == qw):
                        if qw + 's' == ew or ew + 's' == qw:
                            fuzzy_score = fuzz.ratio(qw, ew)
                        else:
                            continue
                    else:
                        continue
                    # If the qw exists in dictionary, then make penalty a bit worse because we'd lke to
                    # favor the entity with the matching word instead of the entity that has almost the matching word
                    if self.word_idf_dict.get(qw, None):
                        fuzzy_score -= (100 * config.LOOKUP_FUZZY_PENALTY)

                    # idf is minimum of qw, ew and cutoff value.
                    # Also check against lemma and use that if we have it and its idf is lower than query word
                    # e.g., we want to use 'wine' and not 'wined' if there isn't an exact match
                    try:
                        lemma = lemmas[qw]
                    except KeyError as e:
                        logger.warning(f'Lemma for word not found: "{qw}"')
                        logger.warning(f'Lemmas are: {lemmas}')
                        logger.warning(f'words_in_query: {words_in_query}, '
                                       f'words_in_entity: {words_in_entity}, '
                                       f'is_allow_fuzzy: {is_allow_fuzzy}')
                        lemma = qw
                        # raise e
                    query_lemma_idf = self.word_idf_dict.get(lemma, 0)
                    query_word_idf = self.word_idf_dict.get(qw, 0)
                    entity_word_idf = self.word_idf_dict.get(ew, 0)
                    if query_lemma_idf > 0 and (
                            query_lemma_idf < query_word_idf or query_word_idf == 0):
                        idf = min(query_lemma_idf, entity_word_idf,
                                  config.LOOKUP_HIGHER_IDF_CUTOFF)
                    elif query_word_idf > 0:
                        idf = min(query_word_idf, entity_word_idf,
                                  config.LOOKUP_HIGHER_IDF_CUTOFF)
                    else:
                        idf = min(entity_word_idf,
                                  config.LOOKUP_HIGHER_IDF_CUTOFF)

                    # query and candidate words have to be within 1 characters in length for safety
                    max_length_diff = 1
                    length_diff = abs(len(qw) - len(ew))
                    if fuzzy_score > config.LOOKUP_FUZZY_THRESHOLD and length_diff <= max_length_diff and qi not in indices_matched:
                        matched_words.append(
                            {'query_indx': qi, 'cand_indx': ei,
                             'score': fuzzy_score / 100.0,
                             'token': ew, 'query_token': qw, 'idf': idf})
                        match_found = True
                        indices_matched.append(qi)
                        break
            # Don't count common word as unmatched
            if match_found is False:
                # Calculate idf for unmatched word which will be minimum of query word and high cutoff
                # We use the high cutoff because if we don't even find query word we still want to penalize
                # idf = min(self.word_idf_dict.get(qw,999), config.LOOKUP_HIGHER_IDF_CUTOFF)
                idf = self.word_idf_dict.get(ew, 0)
                unmatched_words.append(
                    {'query_indx': qi, 'cand_indx': -1, 'score': 0.0,
                     'token': ew, 'idf': idf})

        return matched_words, unmatched_words

    def get_candidate(self, query, entity_id, disallow_brand,
                      is_allow_fuzzy) -> dict:
        """
        Get candidate for the query
        :param query:
        :param entity_id:
        :param disallow_brand:
        :param is_allow_fuzzy:
        :return: bool
        """
        entity_obj = self.entities_dict.get(entity_id, None)
        if not entity_obj:
            return {}
        is_brand = entity_obj['attribute_code'] == 'brand'
        if disallow_brand and is_brand:
            return {}

        matched_words, unmatched_words = self.get_matched_unmatched_words(
            query.split(),
            entity_obj['words'],
            is_allow_fuzzy,
            is_brand)
        if not matched_words:
            return {}
        else:
            # Do a check on brand. If we have a brand and that brand is not sold by the current source, then we require that
            # all of the matched words for the brand must be spelled correctly (although we don't necessarily require all words)
            # So if we had the brand "Manya" but not sold by the store, and our input was "many", that would not match
            if is_brand:
                if any([word for word in matched_words if
                        word['score'] < 1.0]):
                    return {}
        return {'id': entity_id,
                'text': entity_obj['original_text_value'],
                'matched_words': matched_words,
                'is_require_all_words': entity_obj.get('is_require_all_words'),
                'unmatched_words': unmatched_words,
                'word_count': entity_obj['word_count'],
                'entity_id': entity_obj['entity_id']}

    def get_candidate_entities(self, words_in_query, entities_ids,
                               source_brand_list, disallow_brand,
                               is_allow_fuzzy, is_human):
        candidates = []

        for ent in entities_ids:
            # TODO use check_candidate here
            entity_obj = self.entities_dict.get(ent, None)
            if not entity_obj:
                continue
            is_brand = entity_obj['attribute_code'] == 'brand'
            # Skip over brand entity if we are disallowed to have brand
            if disallow_brand and is_brand:
                continue

            matched_words, unmatched_words = self.get_matched_unmatched_words(
                words_in_query,
                entity_obj['words'],
                is_allow_fuzzy,
                is_brand)
            if matched_words:
                # Do a check on brand. If we have a brand and that brand is not sold by the current source, then we require that
                # all of the matched words for the brand must be spelled correctly (although we don't necessarily require all words)
                # So if we had the brand "Manya" but not sold by the store, and our input was "many", that would not match
                # But we only restrict for human interacions ... we can be more flexible for pipeline processing because
                # we should assume that the vast majority of times the brand names will be legal
                if is_brand and is_human:
                    if entity_obj['entity_id'] not in source_brand_list:

                        less_than_two_words = len(entity_obj['words']) < 2

                        missing_words = sorted(entity_obj['words']) != sorted(
                            [w['token'] for w in matched_words]
                        )
                        
                        fuzzy_match = any([w for w in matched_words if
                                           w['score'] < 1.0])

                        if less_than_two_words or missing_words or fuzzy_match:
                            continue

                candidates.append(
                    {'id': ent,
                     'text': entity_obj['original_text_value'],
                     'matched_words': matched_words,
                     'is_require_all_words': entity_obj.get(
                         'is_require_all_words'),
                     'unmatched_words': unmatched_words,
                     'word_count': entity_obj['word_count'],
                     'entity_id': entity_obj['entity_id']}
                )

        return candidates

    def score_candidates(self, words_in_query, candidates, cats,
                         all_match_words, source_brand_list):
        curr_max_score = -1
        scored_candidates = []
        for cand in candidates:
            matched_score = 0
            unmatched_score = 0
            # found_fuzzy_match = False
            try:
                attr_code = self.entities_dict[cand['id']]['attribute_code']
            except BaseException:
                raise
            matched_words = cand['matched_words']
            unmatched_words = cand['unmatched_words']
            matched_word_count = len(matched_words)
            unmatched_word_count = len(unmatched_words)

            # If brand and only one word attribute, must match exactly if < X chars
            min_word_letters = 5
            if attr_code == 'brand' \
                    and cand['word_count'] == 1 \
                    and matched_word_count == 1 \
                    and matched_words[0]['score'] != 1.0 \
                    and len(matched_words[0]['token']) <= min_word_letters:
                cand['final_score'] = -1
                cand['brand_one_word_needs_correct_spelling'] = True
                continue

            # Special case check if candidate only has low matched idf words, then we require a perfect match
            # (no unmatched words and no fuzzy match)
            found_higher_idf_words = [word for word in matched_words if word[
                'idf'] > config.LOOKUP_MIN_IDF_CUTOFF]
            fuzzy_matched_words = [word for word in matched_words if
                                   word['score'] < 1.0]
            found_fuzzy_match = len(fuzzy_matched_words) > 0
            if (len(found_higher_idf_words) == 0) \
                    and (
                    unmatched_word_count > 0 or len(fuzzy_matched_words) > 0):
                cand['final_score'] = -1
                cand['all_low_idf_not_all_words_founds'] = True
                continue

            # Special case if term has any unmmatched words > min idf cutoff and no higher matched words
            found_highest_idf_matched = [word for word in matched_words if
                                         word[
                                             'idf'] > config.LOOKUP_MIN_IDF_CUTOFF]
            found_highest_idf_unmatched = [word for word in unmatched_words if
                                           word[
                                               'idf'] > config.LOOKUP_MIN_IDF_CUTOFF]
            if len(found_highest_idf_matched) == 0 and len(
                    found_highest_idf_unmatched) > 0:
                cand['final_score'] = -1
                cand['found_high_idf_unmatched_in_brand'] = True
                continue

            # If has requires all words flag, then make sure there
            # are no unmatched words
            if cand.get('is_require_all_words') and unmatched_word_count > 0:
                cand['final_score'] = -1
                cand['brand_did_not_have_all_required_words'] = True
                continue

            # If a brand has a word that is an attribute name, require that it includes all lower IDF words
            #  This is to avoid problems like "price vineyards" vs. "price".
            if attr_code == 'brand' and (
                    unmatched_word_count > 0 or found_fuzzy_match):
                found_attr_words = [word for word in matched_words if
                                    (word['token'] in all_match_words or word[
                                        'query_token'] in all_match_words)]
                if found_attr_words:
                    cand['final_score'] = -1
                    cand['brand_had_attr_word_and_unmatched_words'] = True
                    continue

            # Check to see if matching brand has at least a word of > X chars if there are unmatched_words
            if attr_code == 'brand' and unmatched_word_count > 0:
                min_length_to_match = 4
                if max([len(word['token']) for word in
                        matched_words]) < min_length_to_match:
                    cand['final_score'] = -1
                    cand['brand_has_too_short_word_match'] = True
                    continue

            # If we only have one word match and idf of unmatched is high, then skip
            if matched_word_count == 1 and unmatched_word_count > 0:
                high_unmatched_idf = max(
                    [word['idf'] for word in unmatched_words])
                if high_unmatched_idf > config.LOOKUP_HIGHER_IDF_CUTOFF:
                    cand['final_score'] = -1
                    cand['one_word_match_with_high_idf_unmatched'] = True
                    continue

            # If we only have one word match and at least two words unmatched, then require that the
            # one matching word is spelled correctly
            if matched_word_count == 1 and unmatched_word_count >= 2:
                if matched_words[0]['score'] < 1.0:
                    cand['final_score'] = -1
                    cand['one_word_match_not_perfect'] = True
                    continue

            # Special check if matched word includes a category name (e.g., wine or wines) and there
            # are also unmatched words, then don't give credit for category word
            found_cat_words = [word for word in matched_words if
                               word['token'] in cats]
            if len(found_cat_words) > 0 and unmatched_word_count > 0:
                for word in found_cat_words:
                    word['idf'] = 0

            tf = 1  # ignore term frequency
            for index, word in enumerate(matched_words):
                # Logic to deal with getting lowest idf of query and candidate word if fuzzy match
                # if word['score'] < 1.0:
                #    found_fuzzy_match = True

                word['adjusted_idf'] = word['idf'] * word['idf']
                # If word is in common list, then we'll reduce penalty
                common_adjust = (1 - config.LOOKUP_COMMON_PENALTY) if (
                        word['token'] in config.LOOKUP_COMMON_WORDS) else 1.0
                tfidf_score = tf * word['adjusted_idf'] * common_adjust

                # Square fuzzy score for more penalty
                matched_score += (word['score'] * word['score']) * tfidf_score

            for index, word in enumerate(unmatched_words):
                # we always use idf of candidate term for unmatched words cuz there aren't any matching query words
                word['adjusted_idf'] = word['idf'] * word['idf']
                tfidf_score = tf * word['adjusted_idf']
                # If word is in common list, then we'll reduce penalty
                common_adjust = (1 - config.LOOKUP_COMMON_PENALTY) if (
                        word['token'] in config.LOOKUP_COMMON_WORDS) else 1.0
                unmatched_score += tfidf_score * common_adjust

            # Determine if we have ngram bonus to give
            # ngram_bonus is a bonus we apply to words in a run (ngram)
            # But don't include words with zero idf (category words if there are unmatched words)
            run_length = self.get_run_length(
                [word for word in matched_words if word['idf'] > 0])
            ngram_bonus = (1.0 + (
                    config.LOOKUP_NGRAM_BONUS * run_length)) if run_length > 1 else 1.0
            matched_score *= ngram_bonus

            # Give extra credit if we cover all words with no typos
            perfect_bonus = 1.0
            if not found_fuzzy_match and unmatched_word_count == 0:
                perfect_bonus = config.LOOKUP_PERFECT_BONUS
                if len(words_in_query) == cand['word_count']:
                    cand['is_exact_match'] = True
            else:
                # Since not a perfect match, make sure that at least one matching word has a idf that is above
                # the low (common) threshold we set before. This is to avoid matching something like 'la winery'
                # just because the word 'winery' is in query. Only apply when > 1 word in candidate
                # Let's also include the ngram bonus which is our proxy for a real ngram. If we find an ngram_bonus > 1
                # then let's not disqualify. e.g., 'alexandr valley' --> this would normally get rejected because
                # alexandr is not perfect match and valley is a common word.
                # But since we have the two together, it's ok.
                if len(found_higher_idf_words) == 0 and cand[
                    'word_count'] > 1 and ngram_bonus == 1:
                    # make score negative so it won't get included. Also mark all words as unmatched
                    cand['unmatched_words'].extend(matched_words)
                    cand['matched_words'] = []
                    cand['is_low_idf'] = True
                    matched_score = 0
                    unmatched_score = 10000

                # If we only have low idf unmatched words, reduce unmatched_score because people often omit these
                found_low_idf_unmatched_words = [word for word in
                                                 unmatched_words if
                                                 word[
                                                     'idf'] < config.LOOKUP_MIN_IDF_CUTOFF]
                if len(found_low_idf_unmatched_words) == unmatched_word_count:
                    unmatched_score *= 0.2

            # Adjust scores
            matched_factor = 1.0
            unmatched_factor = 2.0 if attr_code == 'brand' else 1.0

            cand['matched_score'] = matched_score * matched_factor
            cand['unmatched_score'] = unmatched_score * unmatched_factor

            cand['final_score'] = (cand['matched_score'] - cand[
                'unmatched_score']) * perfect_bonus

            # Apply special brand logic, but make sure we haven't already disqualified due to low idfs
            if attr_code == 'brand' and not cand.get('is_low_idf', False):
                # Penalize if a brand to give preference to non-brands
                # Increase penalty if first brand word is later in string
                first_brand_word_pos = matched_words[0]['query_indx']
                brand_pos_penalty = config.LOOKUP_BRAND_PENALTY * (
                        1 + 0.1 * first_brand_word_pos)
                cand['brand_pos_penalty'] = brand_pos_penalty
                cand['final_score'] *= (1 - brand_pos_penalty)

            scored_candidates.append(cand)
            curr_max_score = max(curr_max_score, cand['final_score'])

        scored_candidates = sorted(scored_candidates,
                                   key=lambda x: x['final_score'],
                                   reverse=True)
        return scored_candidates

    @staticmethod
    def rescore_candidates(candidates):
        # Additional logic to move candidates up/down
        # Initially we will probably adjust scores for brands based on whether they are sold by current source_id or not
        if len(candidates) == 0:
            return candidates

        return candidates

    def find_matches(self, query, disallow_brand, is_allow_fuzzy, source_id,
                     ordered_codes,
                     category_id, all_match_words, source_brand_list,
                     attr_codes, is_human):
        # Search for early exit if query is identical to dictionary entry

        exact_match = self.find_exact_match(query, disallow_brand,
                                            ordered_codes, attr_codes)
        if exact_match:
            # print("exact match: ", exact_match['text'])
            return [exact_match]

        # Get the category name(s) - e.g., wine, wines. Because we see this word often in input sentences,
        #  we want to restrict matching when it occurs so we don't match "white wine sauce" to "white wine"
        cats = ['wine', 'wines']

        # modified_query = cleanup_string(query)
        words_in_query = query.split()

        # print(words_in_query)
        # print("get_starting_chr_bigrams", datetime.datetime.now().time())
        chr_ngrams = get_starting_chr_bigrams(words_in_query)
        # duplicate bigrams to avoid duplicate lists
        chr_ngrams = list(set(chr_ngrams))
        # print("get_bigram_list", datetime.datetime.now().time())
        subset_entities_ids = self.get_bigram_lists(chr_ngrams, attr_codes)
        # print("get_candidate_entities", datetime.datetime.now().time())
        candidates = self.get_candidate_entities(words_in_query,
                                                 subset_entities_ids,
                                                 source_brand_list,
                                                 disallow_brand,
                                                 is_allow_fuzzy,
                                                 is_human)
        # print("score_candidates", datetime.datetime.now().time())
        # print('candidate count: ', len(candidates))
        scored_candidates = self.score_candidates(words_in_query, candidates,
                                                  cats, all_match_words,
                                                  source_brand_list)
        # print("done with find_matches", datetime.datetime.now().time())
        final_scored_candidates = self.rescore_candidates(scored_candidates)

        return final_scored_candidates

    def find_exact_match(self, query, disallow_brand, ordered_codes,
                         attr_codes):
        # This is for simple, fast exact matching of query to one dictionary item
        # Exclude brand if that flag is set and we find brand
        found_ids = self.entities_text_id_dict.get(query, None)
        if not found_ids:
            return None
        # In case we have multiple ids
        found_id = found_ids if isinstance(found_ids, int) else found_ids[0]
        if self.entities_dict[found_id][
            'attribute_code'] == 'brand' and disallow_brand:
            return None
        # Make sure it's in our allowed attribute code list
        if len(attr_codes) > 0 and self.entities_dict[found_id][
            'attribute_code'] not in attr_codes:
            return None

        if isinstance(found_ids, int):
            found_entity = self.entities_dict[found_ids]
        # If we have more than one potential attribute for this key then disambiguate
        elif isinstance(found_ids, list):
            # If we need to limit based on attr_codes
            if len(attr_codes) > 0:
                found_ids = [i for i in found_ids if
                             self.entities_dict[found_id][
                                 'attribute_code'] in attr_codes]

            found_id = self.disambiguate_multiple_entities(found_ids,
                                                           ordered_codes)
            if not found_id:
                return None

            found_entity = self.entities_dict[found_id]

        # Format same as if we did normal search.
        # First get maximum idf of matched words
        max_idf = max(
            [self.word_idf_dict[word] for word in found_entity['words']])

        return {'id': found_entity['id'], 'is_exact_match': True,
                'text': found_entity['original_text_value'],
                'matched_score': 1000.0, 'unmatched_score': 0.0,
                'final_score': 1000.0,
                'matched_words': [{'query_indx': 0, 'idf': max_idf},
                                  {'query_indx': found_entity[
                                                     'word_count'] - 1,
                                   'idf': max_idf}],
                'unmatched_words': []}

    @staticmethod
    def get_top_entity(matched_entities):
        if matched_entities:
            # Don't bother with word distance checks for exact matches
            if matched_entities[0].get('is_exact_match', False):
                return matched_entities[0]
            else:
                skip_entity = False
                for entity in matched_entities:
                    query_indexes = sorted([word['query_indx'] for word in
                                            entity['matched_words']])
                    prev_idx = query_indexes[0] - 1
                    for index in query_indexes:
                        if index > prev_idx + 1:
                            skip_entity = True
                            break
                        else:
                            prev_idx = index
                    if not skip_entity:
                        return entity
        return None

    def convert_to_result(self, entity, query):
        start = entity['matched_words'][0]['query_indx']
        end = entity['matched_words'][-1]['query_indx'] + 1
        original = ' '.join(query.split()[start:end])
        dict_entity = self.entities_dict[entity['id']]
        entity['attribute_id'] = dict_entity['attribute_id']
        entity['attribute_code'] = dict_entity['attribute_code']
        entity['entity_id'] = dict_entity['entity_id']
        entity['start'] = entity['matched_words'][0]['query_indx']
        entity['end'] = entity['matched_words'][-1]['query_indx']
        # Let's include the maximum matched word idf with result to support any downstream analysis that may
        # be impacted by word frequency
        entity['max_idf'] = max(
            [obj['idf'] for obj in entity['matched_words']])
        entity['original'] = original
        return entity

    def get_all_matches_from_query(self, query, stopword_indexes,
                                   is_single_brand, is_disallow_brand,
                                   is_allow_fuzzy,
                                   source_id, category_id, ordered_codes,
                                   all_match_words, source_brand_list,
                                   attr_codes, orig_sentence,
                                   check_for_products,
                                   is_human):
        products, results = [], []
        # Need to track the original query so we can track the extracted word positions correctly
        orig_query_words = query.split()
        query = ' '.join(
            [word if i not in stopword_indexes else UNMATCHABLE for i, word in
             enumerate(query.split())])
        already_processed = list(stopword_indexes)
        words_in_query = query.split()
        if is_allow_fuzzy:
            self.word_lemma_dictionary_for_query = dict(
                zip(words_in_query, words_in_query))
        else:
            self.word_lemma_dictionary_for_query = dict(
                zip(words_in_query, words_in_query))
        matched = self.find_matches(query, is_disallow_brand, is_allow_fuzzy,
                                    source_id, ordered_codes, category_id,
                                    all_match_words, source_brand_list,
                                    attr_codes, is_human)
        while query.strip():
            # Remove any entities below MIN_SCORE
            matched = [e for e in matched if e['final_score'] > MIN_SCORE]
            matched = self.check_top_matches(matched, source_brand_list)
            top = self.get_top_entity(matched)
            # top =
            if not top or not top.get('id') or top.get('final_score',
                                                       -1) < MIN_SCORE:
                break
            # extract top entity
            entity_obj = self.entities_dict.get(top['id'], None)
            is_brand = entity_obj['attribute_code'] == 'brand'
            if is_brand:
                if top['id'] not in source_brand_list:
                    pass
            # If the top match is a brand, then we check to see if that brand (id)
            # is in the source_brand_list. If it is, then we accept it as the selected entity. 
            # If it's not in the list, then we only accept it as the entity if:
            # 
            # All words are included, and
            # There are no fuzzy matches (only exact matches)
            # If it fails 1 or 2, then skip that entity and move on to the next one and evaluate that.
            # 
            # In other words, if they refer to a brand that is not sold by the store then it has to be an exact match.

            results.append(self.convert_to_result(top, query))
            # Remove brands if we already got one and we're only supposed to have one
            if is_single_brand and top['attribute_code'] == 'brand':
                matched = [e for e in matched if self.entities_dict[e['id']][
                    'attribute_code'] != 'brand']
            matched = [e for e in matched if e is not top]
            # remove the words at matched_words.query_idx from the query string.
            top_words_indexes = [w['query_indx'] for w in top['matched_words']]
            already_processed += top_words_indexes
            query = ' '.join(
                [w if i not in already_processed else UNMATCHABLE for i, w in
                 enumerate(orig_query_words)])
            if top.get('is_exact_match'):
                query = UNMATCHABLE  # no remaining words
                break  # exit on exact match

            rescored = []
            for i, ent in enumerate(matched[:]):
                ent_words_indexes = [w['query_indx'] for w in
                                     ent['matched_words']]
                if set(ent_words_indexes).issubset(top_words_indexes):
                    pass
                elif set(ent_words_indexes).intersection(top_words_indexes):
                    #  re-score this one term using score_candidates()
                    cand = self.get_candidate(query, ent['id'],
                                              is_disallow_brand,
                                              is_allow_fuzzy=True)
                    if cand:
                        scored_candidate = self.score_candidates(
                            query.split(),
                            [cand],
                            ('wine', 'wines'),
                            all_match_words,
                            []
                        )
                        if scored_candidate:
                            rescored.append(scored_candidate[0])
                else:
                    rescored.append(ent)
            matched = sorted([e for e in rescored],
                             key=lambda k: k['final_score'], reverse=True)

            is_brand = top['attribute_code'] == 'brand'
            # If we have a brand, then check to see if we have any products.
            is_disallow_brand = is_brand and is_single_brand
            if is_brand and check_for_products:
                products = self.product_lookup2(
                    top['entity_id'],
                    source_id,
                    category_id,
                    orig_sentence
                )
        product_ids = [p['master_product_id'] for p in products]
        remaining_words_indexes = [i for i, w in enumerate(query.split()) if
                                   w != UNMATCHABLE]
        return results, product_ids, set(remaining_words_indexes)

    def check_top_matches(self, matched_entities, source_brand_list):
        # Do a special check if top 2 matches are brands and within a certain % of each other, but the 2nd brand
        #  is in the current source, while the 1s brand is not, then swap 1 and 2.
        if len(matched_entities) >= 2:
            is_brand1 = self.entities_dict[matched_entities[0]['id']][
                            'attribute_code'] == 'brand'
            is_brand2 = self.entities_dict[matched_entities[1]['id']][
                            'attribute_code'] == 'brand'
            if is_brand1 and is_brand2:
                min_brand_score = min(matched_entities[0]['final_score'],
                                      matched_entities[1]['final_score'])
                max_brand_score = max(matched_entities[0]['final_score'],
                                      matched_entities[1]['final_score'])
                score_ratio = min_brand_score / max_brand_score
                min_score_ratio = 0.70  # must be within 30% of each other
                if score_ratio > min_score_ratio and \
                        matched_entities[0][
                            'entity_id'] not in source_brand_list and \
                        matched_entities[1]['entity_id'] in source_brand_list:
                    matched_entities[0], matched_entities[1] = \
                        matched_entities[1], matched_entities[0]
        return matched_entities

    def lookup(self, source_id, s, is_single_brand=True, is_disallow_brand=False,
               is_allow_fuzzy=False, ordered_codes=None, all_match_words=None,
               source_brand_list=None, attr_codes=None, check_for_products=False,
               is_human=False):
        from application.db_extension.routines import get_default_category_id
        orig_sentence = s
        category_id = get_default_category_id()
        if ordered_codes is None:
            ordered_codes = []
        if all_match_words is None:
            all_match_words = []
        if source_brand_list is None:
            source_brand_list = []
        if attr_codes is None:
            attr_codes = []
        attr_codes = [x for x in attr_codes if x]
        attr_codes = [] if not attr_codes else attr_codes
        product_ids = []
        # the input query will be cleaned, but still may include stopwords. we will send in the original query string
        # and the stopword positions so we can track what words we're removing
        _, stopword_indexes = remove_stopwords(s)
        matched, product_ids, remaining_word_indexes = self.get_all_matches_from_query(
            query=s,
            stopword_indexes=stopword_indexes,
            is_single_brand=is_single_brand,
            is_disallow_brand=is_disallow_brand,
            is_allow_fuzzy=is_allow_fuzzy,
            category_id=category_id,
            source_id=source_id,
            ordered_codes=ordered_codes,
            all_match_words=all_match_words,
            source_brand_list=source_brand_list,
            attr_codes=attr_codes,
            orig_sentence=orig_sentence,
            check_for_products=check_for_products,
            is_human=is_human)

        # return found attributes and indexes of not found (or removed via stopword) indexes
        # Also, return any products we may have
        return_attrs = self.format_as_predicate_syntax(matched)
        return_extra_words = remaining_word_indexes.union(stopword_indexes)
        orig_word_list = s.split()
        return_extra_words = [orig_word_list[i] for i in return_extra_words]
        self.word_lemma_dictionary_for_query = {}
        return return_attrs, product_ids, return_extra_words

    def update_dictionary_lookup_data(self, log_function=logger.info):
        log_function('starting dictionary lookup data update')
        start_time = datetime.now()
        log_function('getting existing index')
        existing_index = {}
        log_function('getting entities')
        data = get_dict_items_from_sql()
        data = convert_to_dict_lookup(data, log_function=log_function)
        log_function('creating ngram index')
        self.ngram_inverted_index = create_ngrams(data, existing_index)
        log_function('processing dictionary')
        res = process_dictionary(data, log_function=log_function)
        (idf_dict, ordered_entities_dict, entities_text_id_dict, _) = res
        self.word_idf_dict = idf_dict
        self.entities_text_id_dict = entities_text_id_dict
        self.entities_dict = ordered_entities_dict
        log_function('finished dictionary update in %s',
                     datetime.now() - start_time)
        self.last_time_dictionary_updated = datetime.now()


dictionary_lookup = DictionaryLookupClass()
