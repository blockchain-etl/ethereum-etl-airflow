from __future__ import print_function

import logging
import os

from ethereumetl_airflow.build_clean_dag import build_clean_dag

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

DAGS_FOLDER = os.environ.get('DAGS_FOLDER', '/home/airflow/gcs/dags')
table_definitions_folder = os.path.join(DAGS_FOLDER, 'resources/stages/parse/table_definitions/*')

# airflow DAG
# This DAG is used to export data from existing table with verified contracts
DAG = build_clean_dag(
    dag_id='ethereum_clean_dag',
    table_definitions_folder=table_definitions_folder,
    schedule_interval='0 23 * * *'
)
