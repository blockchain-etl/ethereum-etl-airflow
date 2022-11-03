from __future__ import print_function

import logging
import os
from glob import glob

from ethereum_parse_v2_datasets import PARSE_V2_DATASETS
from ethereumetl_airflow.build_parse_v2_dag import build_parse_v2_dag
from ethereumetl_airflow.variables import read_parse_dag_vars

DAGS_FOLDER = os.environ.get('DAGS_FOLDER', '/home/airflow/gcs/dags')
table_definitions_folder = os.path.join(DAGS_FOLDER, 'resources/stages/parse/table_definitions/*')

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

var_prefix = 'ethereum_'

for folder in glob(table_definitions_folder):
    dataset = folder.split('/')[-1]

    if dataset not in PARSE_V2_DATASETS:
        continue

    dag_id = f'ethereum_parse_v2_{dataset}_dag'
    logging.info(folder)
    logging.info(dataset)
    globals()[dag_id] = build_parse_v2_dag(
        dag_id=dag_id,
        dataset_folder=folder,
        **read_parse_dag_vars(
            var_prefix=var_prefix,
            dataset=dataset,
            schedule_interval='0 14 * * *'
        )
    )
