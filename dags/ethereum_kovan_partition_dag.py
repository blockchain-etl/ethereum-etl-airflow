from __future__ import print_function

import logging

from ethereumetl_airflow.build_partition_dag import build_partition_dag

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

# airflow DAG
DAG = build_partition_dag(
    dag_id='ethereum_kovan_partition_dag',
    partitioned_project_id='blockchain-etl-kovan-internal',
    partitioned_dataset_name = 'crypto_ethereum_partitioned',
    public_project_id = 'public-data-finance',
    public_dataset_name = 'crypto_ethereum_kovan',
    load_dag_id='ethereum_kovan_load_dag',
    schedule_interval='30 13 * * *'
)
