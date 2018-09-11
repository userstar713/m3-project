#!/usr/bin/env python
import json
from pathlib import Path
from csv import DictReader

from application import create_app
from application.db_extension.models import (db, DomainTaxonomyNode)

ID = 12345
FILENAME = Path(__file__).parent / 'dtn.csv'
FIELDNAMES = [
    'id',
    'attribute_id',
    'parent_id',
    'name',
    'score',
    'aliases',
    'ancestor_node_path',
    'definition',
    'updated',
    'story_text'
]
if __name__ == "__main__":
    app = create_app()
    with app.app_context():
        with open(FILENAME, 'r') as f:
            rdr = DictReader(f, fieldnames=FIELDNAMES)
            for i, raw in enumerate(rdr):
                data = {
                    'attribute_id':raw['attribute_id'],
                    'parent_id':raw['parent_id'],
                    'name':raw['name'],
                    'score':float(raw['score'] or 0.),
                    'definition':raw['definition'],
                    'story_text':raw['story_text'],
                    'ancestor_node_path': raw['ancestor_node_path']
                }
                #q = db.session.query(table).filter_by(source_id=ID)
                #if raw['name'] == 'Central Valley California':
                #    print(data)
                #    raise
                obj = DomainTaxonomyNode(**data)
                db.session.add(obj)
                db.session.commit()
                if i and not i % 10000:
                    break
                    print(i)
        print('Commiting')
        db.session.commit()