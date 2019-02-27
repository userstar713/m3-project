#!/usr/bin/env python
# -*- coding: utf-8 -*-
import base64
import datetime as dt
import json
import re
import html
from collections import defaultdict
from typing import Optional, Union
from unidecode import unidecode
from application.caching import cache

from application.db_extension.dictionary_lookup import config

from .postgres_functions import (
    get_nlp_synonyms,
    get_nlp_ngrams,
    get_attribute_predicates,
    get_attributes)


def listify(arg):
    """
        arg should be an list but if not then make it a list.
    """
    if not isinstance(arg, list):
        arg = [arg, ]
    return arg


def get_max_intent(all_intents):
    final_intent, max_intent = max(all_intents.items(), key=lambda x: x[1])
    final_intent = final_intent.replace("_", "-")
    final_intent = final_intent or None
    max_intent = max_intent or 0

    return final_intent, max_intent


def get_intent_override(s, search_intent, orig_intent, search_score, orig_score,
                        tokenized_words, processed_labels, output):
    # If intent is materially better than non-intent then force intent
    # if search_score - orig_score > 0.2 and orig_score < 0.8:
    #    return 1
    #    return 1

    # If we have a big difference then select the highest one
    # (This overrides top-level classifier if big different between two)
    if orig_score > 0.65 and orig_score > search_score + 0.1:
        return 0

    if search_score > 0.65 and search_score > orig_score + 0.1:
        return 1

    if orig_score > 1.8 * search_score:
        return 0

    if search_score > 1.8 * orig_score:
        return 1

    # If we have a find-product but there are lots of words that are unknown and the
    # the non-attr intent is at least an ok score, then we're going to assume it's non-attr intent
    # This is to compensate for model's tendency to choose find-product
    attr_objs = [obj for obj in output if obj['type']
                 in ['attribute', 'price']]
    if not len(attr_objs) < 3 and orig_score > 0.35 and search_score < 0.9:
        word_count = len(processed_labels)
        unknown_word_count = len(
            [flag for flag in processed_labels if flag is False])
        ratio = unknown_word_count / word_count
        if unknown_word_count > 4 and ratio >= 0.75:
            return 0

    return None


def does_intersect_with_attribute_value(s, pos, all_attributes):
    if not s or not all_attributes:
        return False

    found = any(attr['start'] <= pos <= attr['end'] and
                (
        re.search(r'\b' + s + r'\b', attr['value'], re.IGNORECASE)
        or re.search(r'\b' + s + r'\b', attr['original'], re.IGNORECASE)
    )
        for attr in all_attributes)
    return found


def modify_string_for_context_attribute_if_necessary(cleaned_string, attr_code):
    # not currently used
    """
    # This is to handle the case where user enters "low" instead of "low acidity"


    # Special case check for a single number and add "$" if no there

    #str = (cleaned_string+'.')[:-1]   # really, this can't be the best way to copy a string!
    #if cleaned_string.replace('.','',1).isdigit():
    #    # special case for someone entering '1' or '2' which are often for options
    #    if cleaned_string not in ['1','2']:
    #        return '$'+cleaned_string


    attribute_names, _ = get_attributes()

    # only remove period at end of string
    result = ''.join(c for c in cleaned_string if c not in punctuation or c in ['.','$'])
    if result.endswith('.'):
        result = result[:-1].rstrip()

    words = result.split()
    if not attr_code or len(words) == 0:
        return cleaned_string
    else:
        attr = filter(lambda a: a['code'] == attr_code, attribute_names)
        if attr:
            modified_search_str = words[-1] + ' ' + next(attr)['name']
            obj = domain_attribute_lookup(modified_search_str, 0)[0]
            attrs = obj['attributes']
            if not obj['remaining_words'] and len(attrs) == 1:
                result = (' '.join(words[:-1]) + ' ' + attrs[0]['value']).strip()

        return result

    """
    return cleaned_string


def replace_with_nlp_synonyms(cleaned_string):
    nlp_synonyms = get_nlp_synonyms()
    for nlps in nlp_synonyms:
        # pattern = r'^{}\b' if nlps['must_be_at_beginning'] else r'\b{}\b' if nlps['is_whole_word'] else r'{}'
        # rg = pattern.format(nlps['source_text'])
        cleaned_string = re.sub(
            nlps['rg'], nlps['target_text'], cleaned_string, flags=re.I)
    return cleaned_string.lower()


@cache.memoize()
def replace_with_nlp_ngrams(cleaned_string):

    # TODO: MB commented this out and replaced with simple replace. This regex doesn't seem to work for 'red blend'
    # ngrams, regex = get_nlp_ngrams_regex()
    # return regex.sub(lambda mo: ngrams[mo.string[mo.start():mo.end()]], cleaned_string)

    # for nlps in nlp_synonyms:
    #    # pattern = r'\b{}\b'
    #    # rg = pattern.format(nlps['source_text'])
    #    # cleaned_string = re.sub(rg, nlps['target_text'], cleaned_string, flags=re.I)
    #    # Note: because this is simple replacement, don't need regex and much much faster
    #    # cleaned_string = cleaned_string.replace(nlps['source_text'], nlps['target_text'])
    #    cleaned_string = re.sub(r"\b%s\b" % nlps['source_text'], nlps['target_text'], cleaned_string)
    ngrams = get_nlp_ngrams()
    for key, val in ngrams.items():
        cleaned_string = cleaned_string.replace(val, key)

    return cleaned_string


def add_space_before_period_and_number(text):
    rg = r'\d+\.(?!\d)'
    matches = re.finditer(rg, text)
    for m in matches:
        text_found = m.group(0)
        text = text.replace(text_found, text_found.replace('.', ' .'))

    return text


def cleanup_string(input_str, check_synonyms=True):
    # Make sure it's a string and convert if not
    input_str = str(input_str)

    cleaned_string = input_str.replace('?', ' ?')
    cleaned_string = add_space_before_period_and_number(cleaned_string)

    # Remove last char if common noise
    cleaned_string = cleaned_string.rstrip('/,.')

    # special case of $123-$234 (or $123 - $234)
    cleaned_string = re.sub(r'(.*\d)(?:\s*\-\s*)(\$?\d.*)',
                            r'\1 to \2', cleaned_string)

    cleaned_string = cleaned_string.replace('-', ' ').lower()

    # remove single quote for possessives - this is due to limitations/differences in data_dictionary in postgres
    # if we move dictionary into Python then we can be smarter about how we handle quotes
    # Note that this assumes we have done similar adjustments when writing to data dictionary
    cleaned_string = re.sub(r'\'s', r's', cleaned_string)

    # "mini recursion" of replace_with_nlp_synonyms to handle 2nd order replacement requirements
    # Also substitute ngrams (e.g., "just in" -> "just_in"
    # For performance reasons, we don't need to check if we know input data is simple/clean (e.g., product catalog attributes)
    if check_synonyms:
        begin_time = dt.datetime.now()
        cleaned_string = replace_with_nlp_synonyms(
            replace_with_nlp_synonyms(cleaned_string))
        #print("nlp_synonyms seconds: ", (dt.datetime.now() - begin_time).total_seconds())
        begin_time = dt.datetime.now()
        cleaned_string = replace_with_nlp_ngrams(cleaned_string).lower()
        #print("nlp_ngrams seconds: ", (dt.datetime.now() - begin_time).total_seconds())

    # Replace "J.J." or "J. J." with "JJ"
    cleaned_string = re.sub(r'(.)\.\s?(.)[\.$]', r'\1\2 ', cleaned_string)

    # Replace ". " with " " like "J. Lohr" -> "J Lohr"; or "st. " with "st "
    cleaned_string = re.sub(r'\.[\W$]', ' ', cleaned_string)

    # Remove comma if surrounded by numbers
    cleaned_string = re.sub(
        r'(?:\d)(,)(?:\d)(.*\d)(,)(\d.*)', r'\1\2', cleaned_string)

    # Remove period if surrounded by alpha chars
    cleaned_string = re.sub(r'([a-z]+)\.([a-z]+)', r'\1 \2', cleaned_string)

    # remove remaining punctuation
    cleaned_string = re.sub(r'([^\s\.\$\w])+', ' ', cleaned_string)

    # get rid of multiple spaces/whitespaces in case left behind
    cleaned_string = re.sub('\s+', ' ', cleaned_string).strip()

    if cleaned_string == '':
        print("ADD to nlp_ngrams: ", input_str)
        cleaned_string = input_str.lower()  # Fallback if stripped of everything

    # if not ''.join(cleaned_string.split()).isalnum():
    #    raise ValueError(cleaned_string)

    return cleaned_string


def check_output_for_extrinsics(output_obj):
    """
    :param output_obj:
    :return: True if any attributes in our output_obj contain an extrinsic attribute
    """
    attributes, _ = get_attributes()

    for obj in output_obj:
        if obj['type'] == 'attribute':
            for attribute in obj['attributes']:
                if attribute['code'] not in ['price', 'qpr', 'qoh'] and is_extrinsic(attribute['code']):
                    return True
    return False


def is_number(s):
    """
    Test if string is number
    :param s: string to test
    :return: True if string is number, False if not
    """
    try:
        float(s)
        return True
    except ValueError:
        return False


def is_extrinsic(code):
    """
    :param code:
    :return: True if the attribute(code) is extrinsic
    """
    if not code:
        return False
    attributes, _ = get_attributes()

    lookup_attr = [attr for attr in attributes if attr['code'] == code]

    # in case of synthetic attributes like $popularity
    if not lookup_attr:
        return False
    else:
        return not lookup_attr[0]['is_intrinsic']


def is_numeric(code):
    """
    :param code:
    :return: True if the attribute(code) is numeric (float or currency)
    """
    if not code:
        return False
    attributes, _ = get_attributes()

    lookup_attr = [attr for attr in attributes if attr['code'] == code]

    if not lookup_attr:
        return False
    else:
        return lookup_attr[0]['datatype'] in ['float', 'currency']


def is_scalar(code):
    """
    :param code:
    :return: True if the attribute(code) is scalar
    """
    if not code:
        return False
    attributes, _ = get_attributes()

    lookup_attr = [attr for attr in attributes if attr['code'] == code]

    if not lookup_attr:
        return False
    else:
        return lookup_attr[0]['datatype'] == 'float'


def combine_same_code_attributes(attributes):
    modified_attribs = defaultdict(list)
    for dict_obj in attributes:
        modified_attribs[dict_obj['code']].append(dict_obj)

    final_attribs = []
    derived_definition = None
    derived_guide_key = None
    derived_guide_expression = None
    derived_guides = None
    for key, values in modified_attribs.items():
        if len(values) == 1:
            final_attribs.extend(values)
        else:
            similar_values = []
            original_values = []
            start = values[0]['start']
            end = values[0]['end']
            remaining_values = []

            for obj in values:
                if obj.get('relation') in (None, 'eq', '$eq', '$in'):

                    # Save the first derived info we come across
                    if not derived_definition and obj.get('derived_definition') and len(
                            obj.get('derived_definition', '')) > 0:
                        derived_definition = obj.get('derived_definition')

                    # This is when we are called from lookup_attribute() - we haven't broken into pieces yet
                    if not derived_guides and obj.get('derived_guides') and len(obj.get('derived_guides', [])) > 0:
                        derived_guides = obj.get('derived_guides')

                    # This is when we are called from attribute_intents()
                    if not derived_guide_key and obj.get('derived_guide_key') and len(
                            obj.get('derived_guide_key', '')) > 0:
                        derived_guide_key = obj.get('derived_guide_key')
                    if not derived_guide_expression and obj.get('derived_guide_expression') and len(
                            obj.get('derived_guide_expression', {})) > 0:
                        derived_guide_expression = obj.get(
                            'derived_guide_expression')

                    if obj.get('relation', None) == '$in':
                        # extend with list vs. append obj
                        similar_values.extend(obj['value'])
                        original_values.extend(obj['original'])
                    else:
                        # Only add if not already there (should do for $in above too, but probably pretty rare)
                        if obj['value'] not in [o for o in similar_values]:
                            similar_values.append(obj['value'])
                            original_values.append(obj['original'])
                else:
                    remaining_values.append(obj)

                start = min(start, obj['start'])
                end = max(end, obj.get('end', start))

            if similar_values:
                to_add = ({'code': key, 'start': start, 'end': end,
                           'value': similar_values if len(similar_values) > 1 else similar_values[0],
                           'original': original_values if len(original_values) > 1 else original_values[0]})

                if derived_definition and len(derived_definition) > 0:
                    to_add['derived_definition'] = derived_definition
                if derived_guides and len(derived_guides) > 0:
                    to_add['derived_guides'] = derived_guides
                if derived_guide_key and len(derived_guide_key) > 0:
                    to_add['derived_guide_key'] = derived_guide_key
                if derived_guide_expression and len(derived_guide_expression) > 0:
                    to_add['derived_guide_expression'] = derived_guide_expression

                final_attribs.append(to_add)

            # If some attributes are only combined by '$in' and nothing is added to them, they remain in a single list
            # and don't get passed into the condition where the operator $in is added. So $in is missed out in that case
            # Therefore, we flatten list of lists for above case here. If list is already of single elements, nothing happens
            try:
                similar_values = sum(similar_values, [])
            except:
                pass

            # set relation to $in if more than one value
            if len(similar_values) > 1:
                # special case if we have two prices, combine as between
                if final_attribs[-1]['code'] == 'price':
                    final_attribs[-1]['relation'] = '$between'
                else:
                    # non-price cases
                    final_attribs[-1]['relation'] = '$in'

                # Flattening list of '$in' relation items into a single list
                new_values = []
                for item in final_attribs[-1]['value']:
                    if isinstance(item, list):
                        new_values.extend(item)
                    else:
                        new_values.append(item)
                final_attribs[-1]['value'] = new_values
            final_attribs.extend(remaining_values)

    return final_attribs


def get_search_string_attributes_in_text(text):
    search_str_atbs = ['want', 'question_does', 'recommend', 'question_is', 'explain', 'define', 'compare', 'what',
                       'what_is', 'what_does', 'attribute', 'attribute_unit', 'category', 'question_mark',
                       'attribute_price',
                       'predicate']
    dict_features = {}
    for elem in search_str_atbs:
        if elem not in dict_features:
            dict_features[elem] = 0
        if elem in text.split():
            dict_features[elem] += 1
    return dict_features


# def classify_intent_old(text):
#    cvx = count_vect.transform([text])
#    dict_features = get_search_string_attributes_in_text(text)
#    dvx = dict_vect.transform(dict_features)
#    stacked_vects = sparse.hstack((dvx, cvx))
#    predicted_intent = intent_clf.predict(stacked_vects)
#    predicted_prob = max(intent_clf.predict_proba(stacked_vects))
#    return predicted_intent[0], max(predicted_prob)


def get_transformed_vector(text, vectorizer):
    return vectorizer.transform([text])


# def classify_intent(text):
#    # cvx = get_transformed_vector(text, vec1)
#    # pred = clf1.predict(cvx)[0]
#    pred = clf1.predict( [text] )[0]
#    return pred


# def classify_attribute_intent(text):
#    # cvx = get_transformed_vector(text, vec_attribute)
#    # pred = clf_attribute.predict(cvx)[0]
#    # pred_prob = max(clf_attribute.predict_proba(cvx))
#    pred = clf_attribute.predict( [text] )[0]
#    pred_prob = max( clf_attribute.predict_proba( [text] ) )
#    # If low confidence, set as unknown
#    if max(pred_prob) < 0.25:
#        pred = 'unknown'
#    return pred, max(pred_prob)


# def classify_non_attribute_intent(text):
#    # cvx = get_transformed_vector(text, vec_non_attribute)
#    # pred = clf_non_attribute.predict(cvx)[0]
#    # pred_prob = max(clf_non_attribute.predict_proba(cvx))
#    pred = clf_non_attribute.predict( [text] )[0]
#    pred_prob = max(clf_non_attribute.predict_proba( [text] ))
#    if max(pred_prob) < 0.25:
#        pred = 'unknown'
#    return pred, max(pred_prob)

def classify_universal_intent(search_str, original_str):
    # Search classifier twice and based on score return best one
    from application.core.file_service import clf_universal_attribute
    pred_attr = clf_universal_attribute.predict([search_str])[0]
    pred_non_attr = clf_universal_attribute.predict([original_str])[0]
    pred_attr_prob = max(
        clf_universal_attribute.predict_proba([search_str])).max()
    pred_non_attr_prob = max(
        clf_universal_attribute.predict_proba([original_str])).max()

    return pred_attr, pred_non_attr, pred_attr_prob, pred_non_attr_prob


def cprint(json_object):
    """
        Print colored JSON object
    :param json_object: string (JSON object)
    """
    json_str = json.dumps(json_object, indent=4, sort_keys=True)
    print(highlight(json_str, lexers.JsonLexer(), formatters.TerminalFormatter()))


def get_unprocessed_chunks(tokenized_words, processed_labels):
    """
    Return `chunks` - lists of not-processed tokens (splitted by processed tokens)
    :param tokenized_words:
    :param processed_labels:
    :return:
    """
    result = []
    group = []
    for i in range(len(tokenized_words)):
        if processed_labels[i]:
            if group:
                result.append(group)
                group = []
        else:
            group.append((tokenized_words[i], i))  # (word, position)
    if group:
        result.append(group)
    return result


def letter_to_word_pos(chunk, letter_pos):
    total_len = 0
    last_pos = 0
    for token, token_pos in chunk:
        total_len += len(token) + 1  # extra space
        if last_pos <= letter_pos < total_len:
            # found
            return token_pos
        last_pos += len(token) - 1
    # error
    return -1


def get_attributes_by_priority(category_id):
    return [attr['code'] for attr in get_attribute_predicates(category_id)]


# Creates a string with all of the known predicate expressions that will be used in domain_attribute_lookup to help find
# the right attribute.
def get_disambiguate_attr_codes(s, category_id):
    # Iterate over each attribute that has a domain_attribute_relationships.predicate_for_disambiguation value. Then:
    # (1) search for that attribute name or alias in str
    # (2) search for a match of the predicate_for_disambiguation regex
    # If either is found then return that attribute code
    output_code = None

    # Iterate over domain_attribute_relationships that have predicate_for_disambiguation
    attr_expressions = [attr for attr in get_attribute_predicates(
        category_id) if attr.predicate_for_disambiguation]
    attr_name_aliases = [attr for attr in get_attributes(category_id)[0] if
                         attr['code'] in [attr['attribute'] for attr in attr_expressions]]

    for attr in attr_name_aliases:
        if not output_code:
            names = attr['alias']
            names.append(attr['name'])
            for name in names:
                if re.search(name, s):
                    output_code = attr['code']
                    break

    if not output_code:
        for attr in attr_expressions:
            re_str = attr['predicate_for_disambiguation']
            found = re.search(re_str, s)
            if found:
                output_code = attr[0]
                break

    return [output_code] if output_code else []


def remove_anchor(response):
    if not response:
        return response

    """
    Replace html <a> tag with hyperlink
    :param response:
    :return:
    """
    pattern = r'<a href="|">.*?</a>'
    response = re.sub(pattern, " ", response).replace('  ', ' ')
    return response


# Function to try to determine whether user's sentence refers to the current context ('local')
# or is a global-level sentence. e.g., "how much are THEY?" implies local.
# "How many red wines do you have?" probably implies global.
# To support this we will have another classifier that we will train. In the meantime, we
# will have a simple hack to guess (replace this with classifier when ready).
def get_intent_context(tokenized_words, processed_labels, entity):
    context = 'global'

    local_word_set = {'them', 'they', 'it'}
    has_local_words = set(tokenized_words).intersection(local_word_set)
    if len(has_local_words) > 0:
        context = 'local'

    return context


def remove_stopwords(text):
    # In addition to removing stopwords, we want to convert punctuation to hashable symbols
    text = text.replace('$', 'dollar')
    text = text.replace('&', 'and')
    words = text.split()

    # remove stopwords
    removed_indexes = set([i for i, word in enumerate(
        words) if word in config.LOOKUP_STOPWORDS])
    remaining_words = [word for i, word in enumerate(
        words) if i not in removed_indexes]
    modified_text = ' '.join(remaining_words)
    return modified_text, removed_indexes


# Currently set to trigrams
def get_starting_chr_bigrams(words):
    return [a[:3] for a in words]


def flatten(l):
    # flatten list
    return [y for x in l for y in x]


def chunkify(l, n):
    # For item i in a range that is a length of l,
    for i in range(0, len(l), n):
        # Create an index range for l of n items:
        yield l[i:i + n]


def safe_decode_base64(s: str) -> Optional[dict]:
    try:
        return base64.b64decode(s)
    except BaseException:
        return None


def safe_load_json(s: Optional[str]) -> Optional[dict]:
    try:
        return json.loads(s)
    except BaseException:
        return None


def remove_diacritics(s):
    s = str(s)
    try:
        s = s.encode('raw_unicode_escape').decode('utf8')
    except BaseException:
        pass
    s = html.unescape(s)
    return unidecode(s)


def equals(a: Union[int, float, str], b: Union[int, float, str]) -> bool:
    if a == b:
        return True
    else:
        return str(a) == str(b)
