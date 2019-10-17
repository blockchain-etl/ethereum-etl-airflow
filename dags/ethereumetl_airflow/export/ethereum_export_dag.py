from __future__ import print_function

from dags.ethereumetl_airflow.export import build_export_dag
from dags.ethereumetl_airflow.variables import read_export_dag_vars

# airflow DAG
DAG = build_export_dag(
    dag_id='ethereum_export_dag',
    **read_export_dag_vars(
        var_prefix='ethereum_',
        export_schedule_interval='0 12 * * *',
        export_start_date='2015-07-30',
        export_max_workers=10,
        export_batch_size=10,
        export_retries=5,
    )
)
