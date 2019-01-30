from __future__ import print_function

import logging

from build_load_dag import build_load_dag
from common import read_load_dag_vars

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

# airflow DAG
DAG = build_load_dag(
    dag_id='ethereum_load_dag',
    chain='ethereum',
    **read_load_dag_vars(
        var_prefix='ethereum_',
        schedule_interval='30 1 * * *'
    )
)
