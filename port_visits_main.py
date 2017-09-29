"""

    Minimal local run:

        python -m port_visits_main --name testvisits38abq \
                                   --anchorages gfw_raw.anchorage_naming_20170928 \
                                   --start-date 2016-02-01 \
                                   --end-date 2016-02-28
            
"""
import logging
from anchorages import port_visits

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    port_visits.run()