from __future__ import print_function

import logging

from ethereumetl_airflow.build_parse_dag import build_parse_dag
from ethereumetl_airflow.variables import read_parse_dag_vars

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)


def build_parse_dag_from_dataset_name(dataset):
    folder = f'/home/airflow/gcs/dags/resources/stages/parse/table_definitions/{dataset}'

    dag_id = f'ethereum_parse_{dataset}_dag'
    # airflow DAG
    dag = build_parse_dag(
        dag_id=dag_id,
        dataset_folder=folder,
        **read_parse_dag_vars(
            var_prefix='ethereum_',
            dataset=dataset,
            schedule_interval='0 14 * * *'
        )
    )

    return dag
