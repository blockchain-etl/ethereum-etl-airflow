import argparse
import logging

from google.cloud import bigquery

from ethereumetl_airflow.parse.parse_dataset_folder_logic import parse_dataset_folder
from ethereumetl_airflow.parse.parse_state_manager import ParseStateManager

# initialize argument parser
parser = argparse.ArgumentParser(
    description="Script for parsing dataset folder with table definitions."
)

parser.add_argument("--project", type=str, help="GCP project ID.", required=True)
parser.add_argument(
    "--dataset_name", type=str, help="Dataset name to be parsed.", required=True
)
parser.add_argument(
    "--dataset_folder", type=str, help="Dataset folder to be parsed.", required=True
)
parser.add_argument(
    "--state_bucket", type=str, help="State bucket.", required=True
)
parser.add_argument(
    "--destination_dataset_project_id", type=str, help="GCP project of the destination dataset.", required=True
)

args = parser.parse_args()

sqls_folder = "dags/resources/stages/parse/sqls"

project = args.project

dataset_name = args.dataset_name
dataset_folder = args.dataset_folder
state_bucket = args.state_bucket
destination_dataset_project_id = args.destination_dataset_project_id

source_project_id = 'bigquery-public-data'
source_dataset_name = 'crypto_ethereum'

bigquery_client = bigquery.Client(project=project)

logging_format = "%(asctime)s - %(name)s [%(levelname)s] - %(message)s"
logging.basicConfig(level=logging.INFO, format=logging_format)

parse_state_manager = ParseStateManager(
    dataset_name=dataset_name,
    state_bucket=state_bucket,
    bucket_path=f"parse/state",
    project=project,
)

parse_dataset_folder(
    bigquery_client=bigquery_client,
    dataset_folder=dataset_folder,
    ds=None,
    parse_state_manager=parse_state_manager,
    source_project_id=source_project_id,
    source_dataset_name=source_dataset_name,
    destination_project_id=destination_dataset_project_id,
    sqls_folder=sqls_folder,
    parse_all_partitions=None,
    only_updated=True,
)
