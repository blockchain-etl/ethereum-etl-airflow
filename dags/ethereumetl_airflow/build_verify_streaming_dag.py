from __future__ import print_function

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)


def build_verify_streaming_dag(
        dag_id,
        destination_dataset_project_id,
        chain='ethereum',
        notification_emails=None,
        start_date=datetime(2018, 7, 1),
        schedule_interval='*/10 * * * *',
        max_lag_in_minutes=15):
    dataset_name = 'crypto_{}'.format(chain)

    environment = {
        'dataset_name': dataset_name,
        'destination_dataset_project_id': destination_dataset_project_id,
        'max_lag_in_minutes': max_lag_in_minutes,
    }

    default_dag_args = {
        'depends_on_past': False,
        'start_date': start_date,
        'email_on_failure': True,
        'email_on_retry': True,
        'retries': 5,
        'retry_delay': timedelta(minutes=5)
    }

    if notification_emails and len(notification_emails) > 0:
        default_dag_args['email'] = [email.strip() for email in notification_emails.split(',')]

    # Define a DAG (directed acyclic graph) of tasks.
    dag = DAG(
        dag_id,
        catchup=False,
        schedule_interval=schedule_interval,
        default_args=default_dag_args)

    dags_folder = os.environ.get('DAGS_FOLDER', '/home/airflow/gcs/dags')

    def add_verify_tasks(task, dependencies=None):
        # The queries in verify/sqls will fail when the condition is not met
        # Have to use this trick since the Python 2 version of BigQueryCheckOperator doesn't support standard SQL
        # and legacy SQL can't be used to query partitioned tables.
        sql_path = os.path.join(dags_folder, 'resources/stages/verify_streaming/sqls/{task}.sql'.format(task=task))
        sql = read_file(sql_path)
        verify_task = BigQueryOperator(
            task_id='verify_{task}'.format(task=task),
            sql=sql,
            use_legacy_sql=False,
            params=environment,
            dag=dag)
        if dependencies is not None and len(dependencies) > 0:
            for dependency in dependencies:
                dependency >> verify_task
        return verify_task

    add_verify_tasks('blocks_have_latest')
    add_verify_tasks('transactions_have_latest')
    add_verify_tasks('logs_have_latest')
    add_verify_tasks('token_transfers_have_latest')
    add_verify_tasks('traces_have_latest')
    add_verify_tasks('contracts_have_latest')
    add_verify_tasks('tokens_have_latest')

    add_verify_tasks('blocks_count')
    add_verify_tasks('transactions_count')

    return dag


def read_file(filepath):
    with open(filepath) as file_handle:
        content = file_handle.read()
        return content
