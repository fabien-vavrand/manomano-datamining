import os
import logging


class Config:
    DATA_PATH = os.getenv('DATA_PATH')
    SITE_URL = os.getenv('SITE_URL')


logging.getLogger().setLevel(logging.INFO)
config = Config()