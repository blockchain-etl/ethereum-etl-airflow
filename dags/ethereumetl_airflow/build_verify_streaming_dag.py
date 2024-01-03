from __future__ import print_function

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)


def build_verify_streaming_dag(
        dag_id,
        destination_dataset_project_id,
        chain='ethereum',
        notification_emails=None,
        verify_partitioned_tables=False,
        extra_streaming_tables=None,
        parse_destination_dataset_project_id='',
        start_date=datetime(2018, 7, 1),
        schedule_interval='*/10 * * * *',
        max_lag_in_minutes=15):
    dataset_name = 'crypto_{}'.format(chain)
    partitioned_dataset_name = 'crypto_{}_partitioned'.format(chain)

    environment = {
        'dataset_name': dataset_name,
        'destination_dataset_project_id': destination_dataset_project_id,
        'internal_project_id': parse_destination_dataset_project_id + '-internal',
        'partitioned_dataset_name': partitioned_dataset_name,
        'max_lag_in_minutes': max_lag_in_minutes,
    }

    default_dag_args = {
        'depends_on_past': False,
        'start_date': start_date,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 2,
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

    def add_verify_tasks(task, dependencies=None, params=None):
        # The queries in verify/sqls will fail when the condition is not met
        # Have to use this trick since the Python 2 version of BigQueryCheckOperator doesn't support standard SQL
        # and legacy SQL can't be used to query partitioned tables.
        sql_path = os.path.join(dags_folder, 'resources/stages/verify_streaming/sqls/{task}.sql'.format(task=task))
        sql = read_file(sql_path)

        combined_params = environment.copy()
        task_id = 'verify_{task}'.format(task=task)
        if params:
            combined_params.update(params)
            serialized_params = '_'.join(params.values()).replace('.', '_')
            task_id = task_id + '_' + serialized_params

        verify_task = BigQueryInsertJobOperator(
            task_id=task_id,
            configuration={"query": {"query": sql, "useLegacySql": False}},
            params=combined_params,
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

    add_verify_tasks('blocks_count')
    add_verify_tasks('transactions_count')

    if verify_partitioned_tables:
        add_verify_tasks('partitioned_logs_have_latest')
        add_verify_tasks('partitioned_traces_have_latest')

    # Use this to verify the lag of a streaming job https://github.com/blockchain-etl/blockchain-etl-streaming by piping a Pub/Sub topic to a BigQuery Table
    # https://cloud.google.com/blog/products/data-analytics/pub-sub-launches-direct-path-to-bigquery-for-streaming-analytics
    if extra_streaming_tables is not None and len(extra_streaming_tables) > 0:
        streaming_table_list = [table.strip() for table in extra_streaming_tables.split(',')]
        for streaming_table in streaming_table_list:
            add_verify_tasks('extra_streaming_tables_have_latest', params={'streaming_table': streaming_table})

    return dag


def read_file(filepath):
    with open(filepath) as file_handle:
        content = file_handle.read()
        return content
