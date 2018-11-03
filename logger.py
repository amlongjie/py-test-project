import logging

logger = logging.getLogger()
handler = logging.FileHandler('app.log', encoding='utf-8')
formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)
