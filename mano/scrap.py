from mano.config import config
from mano.data.scraper import Scraper


if __name__ == '__main__':
    scraper = Scraper(config.SITE_URL, save_path=config.DATA_PATH, n_threads=8)
    scraper.run()
