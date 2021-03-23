from __future__ import print_function

from ethereumetl_airflow.build_export_dag import build_export_dag
from ethereumetl_airflow.variables import read_export_dag_vars

# airflow DAG
DAG = build_export_dag(
    dag_id='ethereum_kovan_export_dag',
    **read_export_dag_vars(
        var_prefix='ethereum_kovan_',
        export_schedule_interval='0 13 * * *',
        export_start_date='2017-03-02',
        export_max_workers=10,
        export_batch_size=10,
        export_retries=5,
        export_daofork_traces_option=False,
        export_genesis_traces_option=False,
    )
)
