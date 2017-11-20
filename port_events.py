import logging
import sys
import os

this_dir = os.path.dirname(__file__)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.info('starting port_events')
    try:
        from anchorages import port_events
        result = port_events.run()
    except StandardError, err:
        logging.exception('Exception in run()')
        raise
    sys.exit(result)