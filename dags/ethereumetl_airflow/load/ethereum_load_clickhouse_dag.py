from __future__ import print_function

import airflow

from dags.ethereumetl_airflow.load.build_load_clickhouse_dag import build_load_clickhouse_dag
from dags.ethereumetl_airflow.variables import read_load_dag_vars

# airflow DAG
DAG = build_load_clickhouse_dag(
    dag_id='ethereum_load_daga2_dag11',
    chain='ethereum',
    **read_load_dag_vars(
        var_prefix='ethereum_',
        load_schedule_interval='0 15 * * *',
        load_start_date='2015-07-30',
        load_max_workers=4,
        load_batch_size=1,
    )
)
