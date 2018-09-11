#!/usr/bin/env python
from application import create_app
from application.db_extension.models import (db,
                                             MasterProductProxy,
                                             SourceProductProxy,
                                             SourceReview,
                                             SourceAttributeValue)
ID = 12345

if __name__ == "__main__":
    app = create_app()
    with app.app_context():
        for table in (MasterProductProxy,
                      SourceProductProxy,
                      SourceReview,
                      SourceAttributeValue):
            q = db.session.query(table).filter_by(source_id=ID)
            print(f'{table.__tablename__}: count {q.count()}')
            q.delete()
        db.session.commit()