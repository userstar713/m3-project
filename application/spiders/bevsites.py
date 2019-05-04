import logging
from application.scrapers.csvurl_scraper import CSVURLScraper

BEVSITES_DATA_URL = 'https://dl.dropbox.com/s/keowy5llbuea3yk/added_review_bevsites.csv'
# Pass in the data_url to csv to parent CSVURLScraper class and that's it


class BevsitesCSVScraper(CSVURLScraper):
    def __init__(self):
        super().__init__(data_url=BEVSITES_DATA_URL)
