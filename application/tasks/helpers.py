import html
from unidecode import unidecode

DATATYPES = {
    'node_id': 'value_node_id',
    'float': 'value_float',
    'currency': 'value_float',
    'text': 'value_text',
    'integer': 'value_integer',
    'boolean': 'value_boolean',
}


def remove_diacritics(s: str) -> str:
    try:
        s = s.encode('raw_unicode_escape').decode('utf8')
    except BaseException:
        pass
    s = html.unescape(s)
    return unidecode(s)