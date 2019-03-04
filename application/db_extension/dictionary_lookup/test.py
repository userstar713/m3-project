if __name__ == '__main__':
    from application import create_app
    app = create_app()
    with app.app_context():
        from application.db_extension.dictionary_lookup.lookup import dictionary_lookup
        from application.db_extension.dictionary_lookup import process_dictionary
        process_dictionary.update_dictionary_lookup_data()
        dictionary_lookup.lookup(query, False, False, True, 1, 31, [], [], [], [], "Silver Oak Napa Valley Cabernet Sauvignon 2014", True, True)
        import pdb
        pdb.set_trace()
