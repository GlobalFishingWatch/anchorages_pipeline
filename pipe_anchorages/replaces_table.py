from google.cloud import bigquery
import argparse, sys

def replace_table(project_id, from_table, to_table):
    client = bigquery.Client(project=project_id)
    # client.delete_table(to_table, not_found_ok=True)
    # print(f"Deleted table '{to_table}'.")

    job = client.copy_table(from_table, to_table)
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
    args = parser.parse_args()
    sys.exit(reeplace_table(project_id=args.project_id, from_table=args.from_table, to_table=args.to_table))
