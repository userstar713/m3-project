#!/usr/bin/env python
from pathlib import Path
from csv import DictReader
from collections import defaultdict
from pprint import pprint

FILENAME = Path(__file__).parent / 'out_descr_reviews.csv'

if __name__ == "__main__":
        with open(FILENAME, 'r') as f:
            rdr = DictReader(f)
            output = defaultdict(list)
            for i, raw in enumerate(rdr):
                descr = raw.pop('description')
                output[descr].append(dict(**raw))
            #pprint(output.items())

        from out_descr_reviews import DATA
        descr, reviews = DATA[0]
        print(descr)
        pprint(reviews)
