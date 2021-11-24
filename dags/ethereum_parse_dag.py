from __future__ import print_function

from glob import glob
import logging
import os

from ethereumetl_airflow.build_parse_dag import build_parse_dag
from ethereumetl_airflow.variables import read_parse_dag_vars

DAGS_FOLDER = os.environ.get('DAGS_FOLDER', '/home/airflow/gcs/dags')
table_definitions_folder = os.path.join(DAGS_FOLDER, 'resources/stages/parse/table_definitions/*')

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

var_prefix = 'ethereum_'

for folder in glob(table_definitions_folder):
    dataset = folder.split('/')[-1]

    dag_id = f'ethereum_parse_{dataset}_dag'
    logging.info(folder)
    logging.info(dataset)
    globals()[dag_id] = build_parse_dag(
        dag_id=dag_id,
        dataset_folder=folder,
        **read_parse_dag_vars(
            var_prefix=var_prefix,
            dataset=dataset,
            schedule_interval='0 14 * * *'
        )
    )
