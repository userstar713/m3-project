import requests
import csv
from typing import List
from .base import BaseScraper

class CSVURLScraper(BaseScraper):
    def __init__(self, data_url):
       self._data_url = data_url

    def run(self, source_id: int, full=True) -> List[dict]:
        response = requests.get(self._data_url)
        content = response.content.decode('utf-8')
        rdr = csv.DictReader(content.splitlines())
        return [row for row in rdr]