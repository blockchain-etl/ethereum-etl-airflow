import airflow
from dags.ethereumetl_airflow.setup.build_clickhouse_setup_dag import build_clickhouse_setup_dag


DAG = build_clickhouse_setup_dag(
    dag_id='ethereum_clickhouse_setup',
    chain='ethereum',
)
