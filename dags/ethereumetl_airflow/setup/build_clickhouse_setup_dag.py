from __future__ import print_function

import os
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from dags.ethereumetl_airflow.utils import _build_setup_table_operator


CLICKHOUSE_URI = Variable.get('clickhouse_uri', '')


def build_clickhouse_setup_dag(
        dag_id,
        chain='ethereum',
        notification_emails=None,
):
    dags_folder = os.environ.get('DAGS_FOLDER', '/home/airflow/gcs/dags')

    default_dag_args = {
        'depends_on_past': False,
        'start_date': datetime(2009, 1, 1),
        'email_on_failure': True,
        'email_on_retry': True,
        'retries': 5,
        'retry_delay': timedelta(minutes=5),
    }

    env = {
        'CHAIN': chain,
        'DAG_FOLDER': dags_folder,
        'CLICKHOUSE_URI': Variable.get('clickhouse_uri', '')
    }

    if notification_emails and len(notification_emails) > 0:
        default_dag_args['email'] = [email.strip() for email in notification_emails.split(',')]

    dag = DAG(dag_id, concurrency=4, default_args=default_dag_args, schedule_interval='@once')

    _build_setup_table_operator(dag=dag, env=env, table_type='master', resource="blocks")
    _build_setup_table_operator(dag=dag, env=env, table_type='master', resource="contracts")
    _build_setup_table_operator(dag=dag, env=env, table_type='master', resource="logs")
    _build_setup_table_operator(dag=dag, env=env, table_type='master', resource="receipts")
    _build_setup_table_operator(dag=dag, env=env, table_type='master', resource="tokens")
    _build_setup_table_operator(dag=dag, env=env, table_type='master', resource="token_transfers")
    _build_setup_table_operator(dag=dag, env=env, table_type='master', resource="traces")
    _build_setup_table_operator(dag=dag, env=env, table_type='master', resource="transactions")

    return dag
