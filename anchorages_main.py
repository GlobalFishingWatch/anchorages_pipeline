"""

    Minimal local run:

        python -m anchorages_main \
            --name test-anchorages-2016-tiny \
            --runner DirectRunner \
            --latlon-filters latlon_filters.json \
            --input-pattern tiny 

            
    Small, filtered dataflow run:

        python -m anchorages_main \
            --name test-anchorages-2016-small \
            --latlon-filters latlon_filters.json \
            --input-pattern small 


    Filtered 2016 dataflow run:

        python -m anchorages_main \
            --name test-anchorages-2016-filtered \
            --input-pattern 2016 \
            --latlon-filters latlon_filters.json 


    Full dataflow run:

        python -m anchorages_main \
            --name test-anchorages-all-years \
            --input-pattern all_years 
"""
import logging
from anchorages import anchorages

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    anchorages.run()