import re
from typing import List, Any


def chunkify(l: List, n: int) -> List:
    # For item i in a range that is a length of l,
    for i in range(0, len(l), n):
        # Create an index range for l of n items:
        yield l[i:i + n]


def listify(arg: Any) -> List:
    """
        arg should be an list but if not then make it a list.
    """
    if not isinstance(arg, list):
        arg = [arg, ]
    return arg

def get_float_number(number):
    if not number:
        return None
    try:
        return float(number or 0)
    except ValueError:
        return None

def remove_duplicates(l: List) -> List:
    """
       Remove duplicate objects from list
    """
    hashes = set()
    result = []
    for item in l:
        h = str(item)
        if h in hashes:
            # skipping
            continue
        else:
            hashes.add(h)
            result.append(item)
    return result

def split_to_sentences(content: str) -> List:
    pattern = r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<![A-Z]\.)(?<=\.|\?)\s'
    return re.split(pattern, content)