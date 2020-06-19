from mano.config import config
from mano.data.manager import DataManager


if __name__ == '__main__':
    manager = DataManager(config.DATA_PATH)
    data = manager.load()
