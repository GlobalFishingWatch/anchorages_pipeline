import logging
from . import anchorages

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    anchorages.run()