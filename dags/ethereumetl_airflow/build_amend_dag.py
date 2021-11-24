from __future__ import print_function

import logging
import os
from datetime import datetime, timedelta

from airflow import models
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery

from ethereumetl_airflow.bigquery_utils import submit_bigquery_job, create_view, read_bigquery_schema_from_file
from ethereumetl_airflow.common import read_file

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)


def build_amend_dag(
    dag_id,
    destination_dataset_project_id,
    chain='ethereum',
    notification_emails=None,
    load_start_date=datetime(2018, 7, 1),
    schedule_interval='0 0 * * *',
):
    if not destination_dataset_project_id:
        raise ValueError('destination_dataset_project_id is required')

    dataset_name = f'crypto_{chain}'

    default_dag_args = {
        'depends_on_past': False,
        'start_date': load_start_date,
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

    dags_folder = os.environ.get('DAGS_FOLDER', '/home/airflow/gcs/dags')

    def add_seed_tasks(task):
        def seed_task():
            client = bigquery.Client()
            job_config = bigquery.LoadJobConfig()
            schema_path = os.path.join(dags_folder, 'resources/stages/seed/schemas/{task}.json'.format(task=task))
            job_config.schema = read_bigquery_schema_from_file(schema_path)
            job_config.source_format = bigquery.SourceFormat.CSV
            job_config.skip_leading_rows = 1
            job_config.write_disposition = 'WRITE_TRUNCATE'
            job_config.ignore_unknown_values = True

            file_path = os.path.join(dags_folder, 'resources/stages/seed/data/{task}.csv'.format(task=task))
            table_ref = client.dataset(project='blockchain-etl-internal', dataset_id='common').table(task)
            load_job = client.load_table_from_file(open(file_path, mode='r+b'), table_ref, job_config=job_config)
            submit_bigquery_job(load_job, job_config)
            assert load_job.state == 'DONE'

        seed_operator = PythonOperator(
            task_id='seed_{task}'.format(task=task),
            python_callable=seed_task,
            execution_timeout=timedelta(minutes=30),
            dag=dag
        )

        return seed_operator

    def add_create_amended_tokens_view_tasks(dependencies=None):
        def create_view_task(ds, **kwargs):

            client = bigquery.Client()

            dest_table_name = 'amended_tokens'
            dest_table_ref = client.dataset(dataset_name, project=destination_dataset_project_id).table(dest_table_name)

            sql_path = os.path.join(dags_folder, 'resources/stages/enrich/sqls/amended_tokens.sql')
            sql = read_file(sql_path)
            print('amended_tokens view: \n' + sql)

            description = 'Tokens amended with data from https://github.com/blockchain-etl/ethereum-etl-airflow/blob/master/dags/resources/stages/seed/data/token_amendments.csv'
            create_view(client, sql, dest_table_ref, description=description)

        create_view_operator = PythonOperator(
            task_id='create_token_amendments_view',
            python_callable=create_view_task,
            provide_context=True,
            execution_timeout=timedelta(minutes=60),
            dag=dag
        )

        if dependencies is not None and len(dependencies) > 0:
            for dependency in dependencies:
                dependency >> create_view_operator
        return create_view_operator

    add_token_amendments_operator = add_seed_tasks('token_amendments')
    add_create_amended_tokens_view_tasks(dependencies=[add_token_amendments_operator])

    return dag
