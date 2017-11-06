import logging
from anchorages import port_visits

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    port_visits.run()