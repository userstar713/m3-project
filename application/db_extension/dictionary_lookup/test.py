def main():
    from application import create_app
    app = create_app()
    with app.app_context():
        from application.db_extension.dictionary_lookup.lookup import dictionary_lookup
        dictionary_lookup.update_dictionary_lookup_data()
        query = "Warre's Vintage Port (375ML half-bottle) 2016"
        res = dictionary_lookup.lookup(31, query)
        import pdb
        pdb.set_trace()

if __name__ == '__main__':
    main()

