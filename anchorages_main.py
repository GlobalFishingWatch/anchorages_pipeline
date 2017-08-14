"""

    Minimal local run:

        python -m anchorages_main \
            --output gs://world-fishing-827/scratch/timh/output/test_anchorages_tiny \
            --input-pattern tiny \
            --skip-visits

    Minimal dataflow run:

        python -m anchorages_main \
            --project world-fishing-827 \
            --job_name test-anchorages-visits-accum-0 \
            --runner DataflowRunner \
            --staging_location gs://world-fishing-827/scratch/timh/output/staging \
            --temp_location gs://world-fishing-827/scratch/timh/temp \
            --setup_file ./setup.py \
            --max_num_workers 5 \
            --output gs://world-fishing-827/scratch/timh/output/test_anchorages_tiny \
            --input-pattern tiny \
            --skip-visits

    Small dataflow run:

        python -m anchorages_main \
            --project world-fishing-827 \
            --job_name test-anchorages-visits-accum-1 \
            --runner DataflowRunner \
            --staging_location gs://world-fishing-827/scratch/timh/output/staging \
            --temp_location gs://world-fishing-827/scratch/timh/temp \
            --setup_file ./setup.py \
            --max_num_workers 200 \
            --output gs://world-fishing-827/scratch/timh/output/test_anchorages_small \
            --input-pattern small \
            --skip-visits

    Full dataflow run:

        python -m anchorages_main \
            --project world-fishing-827 \
            --job_name test-anchorages-accum-2 \
            --runner DataflowRunner \
            --staging_location gs://world-fishing-827/scratch/timh/output/staging \
            --temp_location gs://world-fishing-827/scratch/timh/temp \
            --setup_file ./setup.py \
            --max_num_workers 100 \
            --worker_machine_type n1-highmem-2 \
            --output gs://world-fishing-827/scratch/timh/output/test_anchorages_full \
            --input-pattern all_years
            --skip-visits

"""
import logging
from anchorages import anchorages

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    anchorages.run()