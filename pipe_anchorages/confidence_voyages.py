import argparse
import json
import logging
import sys
import time

from google.cloud import bigquery
from jinja2 import Environment, FileSystemLoader

from pipe_anchorages.utils.bqtools import BigQueryHelper, DatePartitionedTable, Schemas
from pipe_anchorages.utils.ver import get_pipe_ver

logger = logging.getLogger()
env_j2 = Environment(loader=FileSystemLoader("./assets/queries/"))

confidence_meaning = {
    "1": "no stop or gap; only an entry and/or exit",
    "2": "only stop and/or gap; no entry or exit",
    "3": "port entry or exit with stop and/or gap",
    "4": "port entry and exit with stop and/or gap",
}


def run(arguments):
    parser = argparse.ArgumentParser(description="Generates the confidence voyages tables.")
    parser.add_argument(
        "-i",
        "--source",
        help="The BQ source table (Format str, ex: dataset.table).",
        required=True,
    )
    parser.add_argument(
        "-c",
        "--min_confidence",
        help="The minimal confidence to detect the voyages (Format str, ex: 3).",
        required=True,
        choices=list(map(str, [2, 3, 4])),
        type=str,
    )
    parser.add_argument(
        "-o",
        "--output",
        help="The BQ destination table (Format str, ex: project:datset.table).",
        required=True,
    )
    parser.add_argument(
        "-labels",
        "--labels",
        help="Adds a labels to a table (Format: json).",
        required=True,
        type=json.loads,
    )
    parser.add_argument(
        "--project",
        help="The GCP project billed for the processing on this step",
        required=True,
    )
    args = parser.parse_args(arguments)

    start_time = time.time()

    bq_helper = BigQueryHelper(
        bq_client=bigquery.Client(
            project=args.project,
        ),
        labels=args.labels,
    )

    # 1. Validate the existance of the table
    logging.info(f"Creates the confidence voyages table <{args.output}> if it does not exists")
    table = DatePartitionedTable(
        table_id=args.output,
        description=f"""
            Created by pipe-anchorages: {get_pipe_ver()}.
            * Create voyages filter per minimal confidence.
            * https://github.com/GlobalFishingWatch/pipe-research
            * Source: {args.source}
            * Minimal confidence: {args.min_confidence} meaning: {confidence_meaning[args.min_confidence]}.

            A "voyage" is defined as the combination of a vessel's previous port_visit's end and next port_visit's start.
            Every vessel's first voyage has an unknown start, so the `trip_start_*` columns are NULL. Respectively, each vessel's last voyage has an undefined end, so the `trip_end_*` columns are NULL.
            If you want to include a vessel's first (or last) voyage you will have to adjust the trip_start (or trip_end) filter to also include NULL values, e.g:
            ...
            WHERE (trip_start <= '2022-12-31' OR trip_start IS NULL)
        """,  # noqa: E501
        schema=Schemas.load_json_schema("assets/schemas/generate_confidence_voyages.schema.json"),
        partitioning_field="trip_start",
    )
    bq_helper.ensure_table_exists(table)
    bq_helper.update_table(table)

    # Apply template
    template = env_j2.get_template("generate_confidence_voyages.sql.j2")
    query = template.render(
        {
            "port_visits_table": f"{args.output}",
            "min_confidence": args.min_confidence,
        }
    )
    # Run query and calc research positions
    bq_helper.run_query_into_table(
        query=query,
        table=table,
    )

    # ALL DONE
    logger.info(f"All done, you can find the output: {args.output}")
    logger.info(f"Execution time {(time.time()-start_time)/60} minutes")
