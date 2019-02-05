from typing import List


class BaseScraper:
    def run(self, source_id: int, full=True) -> List[dict]:
        raise NotImplementedError
