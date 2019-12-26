from __future__ import print_function

import logging

from ethereumetl_airflow.build_load_etherscan_contracts_dag import build_load_etherscan_contracts_dag
from ethereumetl_airflow.variables import read_load_etherscan_contracts_dag_vars

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

# airflow DAG
DAG = build_load_etherscan_contracts_dag(
    dag_id='ethereum_load_etherscan_contracts_dag',
    **read_load_etherscan_contracts_dag_vars(
        var_prefix='ethereum_',
        schedule_interval='30 17 * * *'
    )
)
