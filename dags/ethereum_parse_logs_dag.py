from __future__ import print_function

from ethereumetl_airflow.build_parse_logs_dag import build_parse_logs_dag
from ethereumetl_airflow.variables import read_parse_logs_dag_vars

# TODO: add check for GCP 

# airflow DAG
DAG = build_parse_logs_dag(
    dag_id='ethereum_parse_logs_dag',
    **read_parse_logs_dag_vars(
        var_prefix='ethereum_',
        schedule_interval='30 12 * * *' # TODO: verify what a good start date is
    )
)
