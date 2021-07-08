from google.cloud import bigquery
from google.cloud.bigquery.job import CopyJobConfig
import argparse, sys

def replace_table(project_id, from_table, to_table, timeout='200'):
    client = bigquery.Client(project=project_id)
    # TODO replace this code with WRITE_TRUNCATE after solving error that throws.
    client.delete_table(to_table, not_found_ok=True)
    print(f"Deleted table '{to_table}'.")

    override_table = CopyJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.copy_table(from_table, to_table, job_config=override_table, timeout=timeout)
    job.result() # Wait for the job to complete.
    print(f"Copy table from '{from_table}' to '{to_table}'.")
    return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Replaces destination table with the source table.')
    parser.add_argument('--project_id', help='The project id. format: str', required=True)
    parser.add_argument('--from_table', help='The BigQuery source table id, format: your_dataset.your_table',
                        required=True)
    parser.add_argument('--to_table', help='The BigQuery destination table id, format: your_dataset.your_table',
                        required=True)
    parser.add_argument('--timeout', help='The timeout of the copy job.')
    args = parser.parse_args()
    print(vars(args))
    sys.exit(replace_table(**vars(args)))
