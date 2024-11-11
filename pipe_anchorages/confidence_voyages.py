"""
Executes the confidence voyages query to create the voyages regarding the confidence.

This script will do:
1- Applies the jinja templating to the query.
2- Creates the destination table in case it doesnt exist.
3- Run the query and save results in destination table.
"""

from jinja2 import Environment, FileSystemLoader
from pipe_anchorages.utils.bqtools import BQTools
from pipe_anchorages.utils.ver import get_pipe_ver
import argparse, json, logging, time, sys

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
    # required=False, default='pipe_ais_v3_alpha_internal.raw_port_event_')
    parser.add_argument(
        "-c",
        "--min_confidence",
        help="The minimal confidence to detect the voyages (Format str, ex: 3).",
        required=True,
        choices=list(map(str, [2, 3, 4])),
        type=str,
    )
    # required=False, default='3')
    parser.add_argument(
        "-o",
        "--output",
        help="The BQ destination table (Format str, ex: project:datset.table).",
        required=True,
    )
    # required=False, default='pipe_static.sunrise')
    parser.add_argument(
        "-labels",
        "--labels",
        help="Adds a labels to a table (Format: json).",
        required=True,
        type=json.loads,
    )
    # default='{"environment":"development","resource_creator":"gcp-composer","project":"core_pipeline","version":"babypipeline-20230423","step":"research","stage":"productive"}')
    args = parser.parse_args(arguments)

    start_time = time.time()

    source = args.source
    project, output = args.output.split(":")
    min_confidence = args.min_confidence
    labels = args.labels

    bq_tools = BQTools(project)

    # 1. Validate the existance of the table
    logging.info(f"Creates the confidence voyages table <{output}> if it does not exists")
    description = f"""
        Created by pipe-anchorages: {get_pipe_ver()}.
        * Create voyages filter per minimal confidence.
        * https://github.com/GlobalFishingWatch/pipe-research
        * Source: {source}
        * Minimal confidence: {min_confidence} meaning: {confidence_meaning[min_confidence]}.

        A "voyage" is defined as the combination of a vessel's previous port_visit's end and next port_visit's start.
        Every vessel's first voyage has an unknown start, so the `trip_start_*` columns are NULL. Respectively, each vessel's last voyage has an undefined end, so the `trip_end_*` columns are NULL.
        If you want to include a vessel's first (or last) voyage you will have to adjust the trip_start (or trip_end) filter to also include NULL values, e.g:
        ...
        WHERE (trip_start <= '2022-12-31' OR trip_start IS NULL)
    """
    schema = bq_tools.schema_json2builder(
        "./assets/schemas/generate_confidence_voyages.schema.json"
    )
    bq_tools.create_tables_if_not_exists(
        output,
        labels,
        description,
        schema,
        ["ssvid", "vessel_id", "trip_id"],
        date_field="trip_start",
    )

    try:
        # Apply template
        template = env_j2.get_template("generate_confidence_voyages.sql.j2")
        query = template.render(
            {
                "port_visits_table": f"{project}.{source}",
                "min_confidence": min_confidence,
            }
        )
        print(query)
        # Run query to calc how much bytes will spend
        bq_tools.run_estimation_query(query, args.output.replace(":", "."), labels)
        # Run query and calc research positions
        bq_tools.run_query(query, args.output.replace(":", "."), labels)
    except Exception as err:
        logger.error(f"confidence_voyages - Unrecongnized error: {err}.")
        sys.exit(1)

    bq_tools.update_table_descr(output, description)

    ### ALL DONE
    logger.info(f"All done, you can find the output: {output}")
    logger.info(f"Execution time {(time.time()-start_time)/60} minutes")
