#!/usr/bin/env python

from pprint import pprint

from application import create_app
from application.db_extension.models import db, DomainAttribute

if __name__ == "__main__":
    app = create_app()
    with app.app_context():
        attributes = db.session.query(DomainAttribute).all()

    pprint(attributes)