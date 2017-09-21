"""

    Minimal local run:

        python -m anchorages_main \
            --name test-anchorages-2016-tiny \
            --runner DirectRunner \
            --latlon-filters latlon_filters.json \
            --input-pattern tiny 

            
"""
import logging
from anchorages import port_visits

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    port_visits.run()