from typing import List

class BaseScraper:
    def run(self) -> List[dict]:
        raise NotImplementedError
