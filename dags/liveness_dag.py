import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta

default_dag_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': airflow.utils.dates.days_ago(0)
}

dag = DAG(
    'liveness_dag',
    default_args=default_dag_args,
    description='liveness monitoring dag',
    schedule_interval=timedelta(minutes=10))

BashOperator(task_id='echo', bash_command='echo test', dag=dag, depends_on_past=False)
