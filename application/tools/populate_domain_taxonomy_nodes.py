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
            rows = sorted([r for r in rdr], key=lambda x: int(x['parent_id']))
            db.session.execute("TRUNCATE TABLE domain_taxonomy_nodes CASCADE;")
            db.session.execute("ALTER SEQUENCE domain_taxonomy_nodes_id_seq RESTART WITH 1;")
            for i, raw in enumerate(rows):
                data = {
                    'attribute_id':raw['attribute_id'],
                    'parent_id':raw['parent_id'] or None,
                    'name':raw['name'],
                    'score':float(raw['score'] or 0.),
                    'definition':raw['definition'],
                    'story_text': '{'+raw['story_text']+'}',
                    #'ancestor_node_path': json.loads(raw['ancestor_node_path'])
                }
                #q = db.session.query(table).filter_by(source_id=ID)
                #if raw['name'] == 'Central Valley California':
                #    print(data)
                #    raise
                try:
                    obj = DomainTaxonomyNode(**data)
                    db.session.add(obj)
                    db.session.commit()
                except BaseException as e:
                    print(e)
                    raise e
                if i and not i % 10000:
                    break
                    print(i)
        print('Commiting')
        db.session.commit()