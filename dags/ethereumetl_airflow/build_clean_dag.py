from __future__ import print_function

import logging
from datetime import datetime, timedelta
from glob import glob

from airflow import models
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import ExternalTaskSensor
from google.cloud import bigquery

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)


def build_clean_dag(
    dag_id,
    table_definitions_folder,
    notification_emails=None,
    schedule_interval='0 0 * * *'
):

    default_dag_args = {
        'depends_on_past': False,
        'start_date': datetime.strptime('2015-07-30', '%Y-%m-%d'),
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 5,
        'retry_delay': timedelta(minutes=5)
    }

    if notification_emails and len(notification_emails) > 0:
        default_dag_args['email'] = [email.strip() for email in notification_emails.split(',')]

    # Define a DAG (directed acyclic graph) of tasks.
    dag = models.DAG(
        dag_id,
        catchup=False,
        schedule_interval=schedule_interval,
        default_args=default_dag_args)

    def add_clean_tasks(task, start_index, end_index, sql_statement_template, dependencies=None):
        def clean_task(ds, **kwargs):
            client = bigquery.Client()

            script = generate_clean_partitioned_logs_script(ds, start_index, end_index, sql_statement_template)
            print(script)
            query_job = client.query(script)
            result = query_job.result()
            logging.info(result)

        clean_operator = PythonOperator(
            task_id=f'clean_{task}',
            python_callable=clean_task,
            provide_context=True,
            execution_timeout=timedelta(hours=4),
            dag=dag
        )

        if dependencies is not None and len(dependencies) > 0:
            for dependency in dependencies:
                dependency >> clean_operator
        return clean_operator

    wait_tasks = []
    for folder in glob(table_definitions_folder):
        dataset = folder.split('/')[-1]
        wait_task = ExternalTaskSensor(
            task_id=f'wait_{dataset}_parse_dag',
            external_dag_id=f'ethereum_parse_{dataset}_dag',
            external_task_id='parse_all_checkpoint',
            execution_delta=timedelta(hours=9),
            priority_weight=0,
            mode='reschedule',
            poke_interval=5 * 60,
            timeout=60 * 60 * 12,
            dag=dag)
        wait_tasks.append(wait_task)

    all_ranges = [
        (0, 1000),
        (1000, 2000),
        (2000, 3000),
        (3000, 4000),
        (4000, 4097),
    ]
    dependency_tasks = wait_tasks
    for start_index, end_index in all_ranges:
        clean_logs_task = add_clean_tasks(f'logs_{start_index}_{end_index}', start_index, end_index, CLEAN_LOGS_SQL_STATEMENT_TEMPLATE)
        clean_traces_task = add_clean_tasks(f'traces_{start_index}_{end_index}', start_index, end_index, CLEAN_TRACES_SQL_STATEMENT_TEMPLATE)
        if dependency_tasks is not None:
            for dependency_task in dependency_tasks:
                dependency_task >> clean_logs_task
                dependency_task >> clean_traces_task
        dependency_tasks = [clean_logs_task, clean_traces_task]

    return dag


CLEAN_LOGS_SQL_STATEMENT_TEMPLATE = 'DELETE FROM `blockchain-etl-internal.crypto_ethereum_partitioned.logs_by_topic_{table_suffix}` WHERE DATE(block_timestamp) <= \'{ds}\';\n'
CLEAN_TRACES_SQL_STATEMENT_TEMPLATE = 'DELETE FROM `blockchain-etl-internal.crypto_ethereum_partitioned.traces_by_input_{table_suffix}` WHERE DATE(block_timestamp) <= \'{ds}\';\n'

def generate_clean_partitioned_logs_script(ds, start_index, end_index, sql_statement_template):
    hex_chars = '0123456789abcdef'

    lines = []

    lines.append(sql_statement_template.format(table_suffix='empty', ds=ds))
    for hex_char1 in hex_chars:
        for hex_char2 in hex_chars:
            for hex_char3 in hex_chars:
                table_suffix = f'0x{hex_char1}{hex_char2}{hex_char3}'
                lines.append(sql_statement_template.format(table_suffix=table_suffix, ds=ds))

    filtered_lines = lines[start_index:end_index]
    return ''.join(filtered_lines)