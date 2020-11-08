from __future__ import print_function

import logging

from ethereumetl_airflow.build_extract_dag import build_extract_dag

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

# airflow DAG
# This DAG is used to export data from existing table with verified contracts
DAG = build_extract_dag(
    dag_id='ethereum_extract_dag',
    schedule_interval='0 16 * * *'
)
