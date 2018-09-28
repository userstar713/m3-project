import json

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


def scoring(structured_data):
    summery_list = []
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
        summery_list.append(
            sorted(summery_dic, key=lambda x: x['score'], reverse=True))
    return summery_list


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


def extract_text(sorted_sum_dic):
    summery_text = ''
    score = 0.0
    for summery in sorted_sum_dic['summery']:
        summery_text += summery['text'] + ' '
        score += summery['score']
    return summery_text.strip(), score


def summarize(structured_data, max_words_len=30):
    summery_list = []
    sum_dic_list = scoring(structured_data)
    for sum_dic in sum_dic_list:
        text, score = extract_text(adopt(sum_dic, max_words_len))
        summery_dic = dict()
        summery_dic['text'] = text
        summery_dic['score'] = score
        summery_list.append(summery_dic)
    summery_list = sorted(summery_list, key=lambda x: x['score'],
                          reverse=True)  # get the highest score summery
    best_summery = summery_list[0]
    summery_text = best_summery['text']
    summery_score = best_summery['score']
    if best_summery['score'] == 0.0 and best_summery['text']!='' and max_words_len!=50:
        summery_text,summery_score = summarize(structured_data, 50)
    return summery_text, summery_score

if __name__ == "__main__":
    with open('output.json', 'r') as f:
        json_data = json.load(f)
        for product in json_data.values():
            summery,score=summarize(product)
            if score==0.0 and summery!='':
                print(json.dumps(summarize(product, 50), indent=4), end="\n\n")
