"""

    Minimal dataflow run:

        python -m anchorages_main \
            --name testanchorages2016tiny \
            --start-date 2016-01-01 \
            --end-date 2016-01-31 \


    2016 dataflow run:

        python -m anchorages_main \
            --name testanchorages2016 \
            --start-date 2016-01-01 \
            --end-date 2016-12-31 \

    Full dataflow run:

        python -m anchorages_main \
            --name testanchoragesallyears \
            --start-date 2012-01-01 \
            --end-date 2017-12-31 \
"""
import logging
from anchorages import anchorages

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    anchorages.run()