if __name__ == '__main__':
    from application import create_app
    app = create_app()
    with app.app_context():
        from application.db_extension.dictionary_lookup.lookup import dictionary_lookup
        from application.db_extension.dictionary_lookup import process_dictionary
        import pdb
        pdb.set_trace()
