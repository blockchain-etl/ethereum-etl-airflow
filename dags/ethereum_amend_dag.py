from __future__ import print_function

import logging

from ethereumetl_airflow.build_amend_dag import build_amend_dag
from ethereumetl_airflow.variables import read_amend_dag_vars

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

# airflow DAG
DAG = build_amend_dag(
    dag_id='ethereum_amend_dag',
    chain='ethereum',
    **read_amend_dag_vars(
        var_prefix='ethereum_',
        schedule_interval='30 12 * * *'
    )
)
