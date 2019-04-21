from __future__ import print_function

import logging
from datetime import datetime

from ethereumetl_airflow.build_load_dag_traces_with_method import build_load_dag_traces_with_method

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

# airflow DAG
DAG = build_load_dag_traces_with_method(
    dag_id='ethereum_load_dag_traces_with_method',
    chain='ethereum',
    load_start_date=datetime.strptime('2015-07-30', '%Y-%m-%d'),
    destination_dataset_project_id='crypto-etl-ethereum-dev'
)

