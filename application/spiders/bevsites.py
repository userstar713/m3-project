import logging
import requests
import csv
from typing import List
from application.scrapers.base import BaseScraper

# from application.scrapers.csvurl_scraper import CSVURLScraper

BEVSITES_DATA_URL = 'https://dl.dropbox.com/s/keowy5llbuea3yk/added_review_bevsites.csv'
# Pass in the data_url to csv to parent CSVURLScraper class and that's it

req = requests.Session()

class BevsitesCSVScraper(BaseScraper):
    
    def run(self, source_id: int, full=True) -> List[dict]:
        with open('added_review_bevsites.csv') as csvfile:
            rdr = csv.DictReader(csvfile, delimiter=',')
        
            return [row for row in rdr]
        
