import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger('seller_integration')
#logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)