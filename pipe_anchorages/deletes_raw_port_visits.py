from google.cloud import bigquery
import argparse, sys

def deletes_table(table_id):
    client = bigquery.Client(project="world-fishing-827")
    client.delete_table(table_id, not_found_ok=True)
    print(f"Deleted table '{table_id}'.")
    return 0

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Deletes the raw_port_visits table to avoid storing lots of space.')
    parser.add_argument('--table_id', help='The BigQuery table id, format: your-project.your_dataset.your_table',
                        required=True)
    args = parser.parse_args()
    sys.exit(deletes_table(table_id=args.table_id))
