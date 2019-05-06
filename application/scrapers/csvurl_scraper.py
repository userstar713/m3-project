# import requests
# import csv
# from typing import List
# from .base import BaseScraper

# class CSVURLScraper(BaseScraper):
#     def __init__(self, data_url):
#        self._data_url = data_url

#     def run(self, source_id: int, full=True) -> List[dict]:
#         # try:
#         #     response = requests.get(self._data_url)
#         # except:
#         #     print('=============================================')
#         # content = response.content.decode('utf-8')
#         # rdr = csv.DictReader(content.splitlines())
#         # return [row for row in rdr]
#         return [1,2,self._data_url]