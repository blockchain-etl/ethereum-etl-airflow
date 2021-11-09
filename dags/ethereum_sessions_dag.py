from __future__ import print_function
from airflow.models import Variable
from datetime import datetime
from ethereumetl_airflow.build_sessions_dag import build_sessions_dag

import logging
import os

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

DAGS_FOLDER = os.environ.get('DAGS_FOLDER', '/home/airflow/gcs/dags')
sql_dir = os.path.join(DAGS_FOLDER, 'resources/stages/sessions/sqls')

environment = Variable.get('environment', 'prod')


# airflow DAG
DAG = build_sessions_dag(
    dag_id='ethereum_sessions_dag',
    load_dag_id='ethereum_load_dag',
    sql_dir=sql_dir,
    source_project_id='bigquery-public-data',
    source_dataset_name='crypto_ethereum',
    destination_project_id='crypto-etl-public-data-dev',  # blockchain-etl-internal
    destination_dataset_name='sessions',
    # Load DAG should complete by 14:00.
    schedule_interval='0 14 * * *',
    start_date=datetime(2015, 7, 30),
    environment=environment
)
