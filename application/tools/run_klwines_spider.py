from application.spiders.klwines import get_data
from pathlib import Path

BASEPATH = Path()

if __name__ == "__main__":
    with open(Path(__file__).parent / 'klwines.txt', 'w') as f:
        get_data(f)