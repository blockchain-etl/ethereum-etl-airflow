from __future__ import print_function

from ethereumetl_airflow.build_load_dag import build_load_dag
from ethereumetl_airflow.variables import read_load_dag_vars

# airflow DAG
DAG = build_load_dag(
    dag_id='ethereum_kovan_load_dag',
    chain='ethereum_kovan',
    **read_load_dag_vars(
        var_prefix='ethereum_kovan_',
        schedule_interval='30 12 * * *'
    )
)

