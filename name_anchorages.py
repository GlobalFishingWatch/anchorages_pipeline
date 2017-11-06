import logging
from anchorages import name_anchorages

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    name_anchorages.run()