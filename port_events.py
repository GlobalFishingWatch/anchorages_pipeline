import logging
from anchorages import port_events

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    port_events.run()
