"""

        python -m name_anchorages_main \
            --name testnameanchorages \
            --input-table machine_learning_dev_ttl_30d.anchorages_testanchorages2016b \
            --output-table machine_learning_dev_ttl_30d.test_anchorage_naming

"""
import logging
from anchorages import name_anchorages

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    name_anchorages.run()