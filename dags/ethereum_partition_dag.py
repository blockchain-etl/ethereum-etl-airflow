from __future__ import print_function

import logging

from ethereumetl_airflow.build_partition_dag import build_partition_dag

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

# airflow DAG
DAG = build_partition_dag(
    dag_id='ethereum_partition_dag',
    schedule_interval='30 13 * * *'
)
