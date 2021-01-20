from __future__ import print_function

import logging
from datetime import datetime, timedelta

from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)


def build_extract_dag(
    dag_id,
    notification_emails=None,
    schedule_interval='0 0 * * *'
):

    bucket = 'ethereum-etl-bigquery'
    default_dag_args = {
        'depends_on_past': False,
        'start_date': datetime.strptime('2021-01-18', '%Y-%m-%d'),
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
        schedule_interval=schedule_interval,
        default_args=default_dag_args)

    def add_extract_tasks(task, dependencies=None):
        def enrich_task(ds, **kwargs):
            client = bigquery.Client()

            query_job = client.query(f'''
            EXPORT DATA OPTIONS (
            uri="gs://{bucket}/export/{task}/block_date={ds}/*.gz", 
                format=CSV, compression=GZIP
            ) AS 
            SELECT *
            FROM `bigquery-public-data.crypto_ethereum.{task}` 
            WHERE DATE(block_timestamp) = '{ds}'
            ''')
            result = query_job.result()
            logging.info(result)

        extract_operator = PythonOperator(
            task_id=f'extract_{task}',
            python_callable=enrich_task,
            provide_context=True,
            execution_timeout=timedelta(minutes=60),
            dag=dag
        )

        if dependencies is not None and len(dependencies) > 0:
            for dependency in dependencies:
                dependency >> extract_operator
        return extract_operator

    def add_compose_tasks(task, dependencies=None):
        compose_task = BashOperator(
            task_id=f'compose_{task}',
            bash_command=f'gsutil compose gs://{bucket}/export/{task}/block_date={{{{ds}}}}/000*.gz gs://ethereum-etl-bigquery/export/{task}/block_date={{{{ds}}}}/{task}.gz && '
                         f'gsutil compose gs://{bucket}/export/{task}/block_date={{{{ds}}}}/000*.gz gs://ethereum-etl-bigquery/export/{task}/block_date=latest/{task}.gz',
            dag=dag,
            depends_on_past=False)
        if dependencies is not None and len(dependencies) > 0:
            for dependency in dependencies:
                dependency >> compose_task
        return compose_task

    entities = ['token_transfers', 'transactions']
    for entity in entities:
        extract_task = add_extract_tasks(entity)
        compose_task = add_compose_tasks(entity, dependencies=[extract_task])

    return dag
