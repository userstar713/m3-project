import json
import re


CONTAIN_VALUE_REG = re.compile(r'.*[0-9][0-9].*')
CONTAIN_FUZZY_BRACKETS_REG = re.compile(r'.*\(.*\).*')
CONTAIN_POINTS_REG = re.compile(r'.*points.*')
CONTAIN_VALUE_BRACKETS_PLUS_REG = re.compile(r'[0-9][0-9]\(.*\)')
CONTAIN_VALUE_PLUS_REG = re.compile(r'.*[4-9][0-9][+].*')
CONTAIN_STRICT_BRACKETS_REG = re.compile(r'\(.*\)')
SENTENCE_SEPARATOR_REG = re.compile(
    r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s')
WEIGHT_DIC = dict()
ATTRIBUTES = ("sku",
              "designation",
              "price",
              "msrp",
              "characteristics",
              "fortified",
              "description",
              "image",
              "date_added",
              "single_product_url",
              "is_vintage_wine",
              "drink_from",
              "drink_to",
              "is_blend",
              "name",
              "acidity",
              "body",
              "tannin",
              "alcohol_pct",
              "sweetness",
              "varietals",
              "popularity",
              "purpose",
              "flaw",
              "vintage",
              "qpr",
              "styles",
              "bottle_size",
              "highlights",
              "color_intensity",
              "discount_pct",
              "foods",
              "rating",
              "product_id",
              "qoh",
              "wine_type",
              "region",
              "brand",
              "prototype",
              "short_desc")

for weight in ATTRIBUTES:
    WEIGHT_DIC[weight.strip()] = 1.0


def is_valid_summary(summary):
    rule_1 = CONTAIN_VALUE_REG.match(summary)
    rule_2 = CONTAIN_FUZZY_BRACKETS_REG.match(summary)
    rule_3 = CONTAIN_POINTS_REG.match(summary)
    rule_4 = CONTAIN_VALUE_BRACKETS_PLUS_REG.match(summary)
    if rule_1 is not None:
        return False
    if rule_2 is not None:
        return False
    if rule_3 is not None:
        return False
    if rule_4 is not None:
        return False
    return True


def is_valid_sentence(sentence):
    rule_3 = CONTAIN_POINTS_REG.match(sentence)
    rule_4 = CONTAIN_VALUE_BRACKETS_PLUS_REG.match(sentence)
    rule_5 = CONTAIN_VALUE_PLUS_REG.match(sentence)
    rule_6 = CONTAIN_STRICT_BRACKETS_REG.match(sentence)

    if rule_3 is not None:
        return False
    if rule_4 is not None:
        return False
    if rule_5 is not None:
        return False
    if rule_6 is not None:
        if rule_6.group() == sentence:
            return False
    return True


def scoring(structured_data):
    summary_list = []
    for data in structured_data:
        summery_dic = []
        for i, sentence in enumerate(data['sentences']):
            score = 0.0
            attribute_count = 0
            text = sentence['text']
            for attribute in sentence['attributes']:
                if attribute['code'] in WEIGHT_DIC.keys():
                    weight = WEIGHT_DIC[attribute['code']]
                else:
                    weight = 1.0
                score += weight
                attribute_count += 1
            if attribute_count != 0:
                score /= attribute_count
            sentence_dic = dict()
            sentence_dic['index'] = i
            sentence_dic['text'] = text
            sentence_dic['score'] = score
            summery_dic.append(sentence_dic)
        summary_list.append(
            sorted(summery_dic, key=lambda x: x['score'], reverse=True))
    return summary_list


def adopt(sentences_dic, max_words_len):
    words_leng = 0
    summery = []
    for sentence_dic in sentences_dic:
        if words_leng + len(sentence_dic['text'].split(' ')) < max_words_len:
            summery.append(sentence_dic)
            words_leng += len(sentence_dic['text'].split(' '))
    summery_dic = dict()
    summery_dic['summery'] = sorted(summery, key=lambda x: x['index'],
                                    reverse=False)
    summery_dic['summery_leng'] = words_leng
    return summery_dic


def remove_unnecessary_information(summary):
    final_result = ''
    if not is_valid_summary(summary):
        sents = SENTENCE_SEPARATOR_REG.split(summary)
        for sent in sents:
            if is_valid_sentence(sent):
                final_result += sent.strip() + ' '
    else:
        final_result = summary

    return final_result.strip()


def extract_text(sorted_sum_dic):
    summery_text = ''
    score = 0.0
    for summery in sorted_sum_dic['summery']:
        summery_text += summery['text'] + ' '
        score += summery['score']
    return summery_text.strip(), score


def summarize(structured_data, max_words_len=50):
    summary_list = []
    sum_dic_list = scoring(structured_data)
    for sum_dic in sum_dic_list:
        text, score = extract_text(adopt(sum_dic, max_words_len))
        summery_dic = dict()
        summery_dic['text'] = text
        summery_dic['score'] = score
        summary_list.append(summery_dic)
    summary_list = sorted(summary_list, key=lambda x: x['score'],
                          reverse=True)  # get the highest score summery
    best_summery = summary_list[0]
    summary_text = best_summery['text']
    summary_score = best_summery['score']
    if (best_summery['score'] == 0.0 and
            best_summery['text'] != '' and
            max_words_len != 50):
        summary_text, summary_score = summarize(structured_data, 50)
    summary_text = remove_unnecessary_information(summary_text)

    return summary_text, summary_score


def main():
    with open('./tools/example_input.json', 'r') as f:
        json_data = json.load(f)
        for product in json_data.values():
            summary, _ = summarize(product)
            if summary != '':
                print(summary)


if __name__ == "__main__":
    main()
