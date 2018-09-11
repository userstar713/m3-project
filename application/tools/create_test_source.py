#!/usr/bin/env python
from application import create_app
from application.db_extension.models import db, Source

ID = 12345

if __name__ == "__main__":
    app = create_app()
    with app.app_context():
        if not db.session.query(Source).get(ID):
            source = Source(id=ID,
                            name='Test Source',
                            source_url='http://example.com')
            db.session.add(source)
            db.session.commit()