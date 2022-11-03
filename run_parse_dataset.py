import logging

from google.cloud import bigquery

from ethereumetl_airflow.parse.parse_dataset_folder_logic import parse_dataset_folder

sqls_folder = 'dags/resources/stages/parse/sqls'

project = '<your_gcp_project>'
dataset = 'aelf'
destination_project_id = 'blockchain-etl-dev'

bigquery_client = bigquery.Client(project=project)

logging_format = '%(asctime)s - %(name)s [%(levelname)s] - %(message)s'
logging.basicConfig(level=logging.INFO, format=logging_format)

parse_dataset_folder(
    bigquery_client=bigquery_client,
    dataset_folder=f'dags/resources/stages/parse/table_definitions/{dataset}',
    ds='2022-11-01',
    source_project_id='bigquery-public-data',
    source_dataset_name='crypto_ethereum',
    destination_project_id=destination_project_id,
    sqls_folder=sqls_folder,
    parse_all_partitions=False,
)