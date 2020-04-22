from __future__ import print_function

import logging
import os
from datetime import datetime, timedelta
from glob import glob

from airflow import models
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.email_operator import EmailOperator
from google.cloud import bigquery

from ethereumetl_airflow.common import read_json_file
from ethereumetl_airflow.parse import create_or_update_table_from_table_definition, ref_regex

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

dags_folder = os.environ.get('DAGS_FOLDER', '/home/airflow/gcs/dags')


def build_parse_dag(
        dag_id,
        dataset_folder,
        parse_destination_dataset_project_id,
        notification_emails=None,
        parse_start_date=datetime(2018, 7, 1),
        schedule_interval='0 0 * * *',
        parse_all_partitions=True,
        send_success_email=False
):
    logging.info('parse_all_partitions is {}'.format(parse_all_partitions))

    if parse_all_partitions:
        dag_id = dag_id + '_FULL'

    SOURCE_PROJECT_ID = 'bigquery-public-data'
    SOURCE_DATASET_NAME = 'crypto_ethereum'

    default_dag_args = {
        'depends_on_past': False,
        'start_date': parse_start_date,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 5,
        'retry_delay': timedelta(minutes=5)
    }

    if notification_emails and len(notification_emails) > 0:
        default_dag_args['email'] = [email.strip() for email in notification_emails.split(',')]

    dag = models.DAG(
        dag_id,
        catchup=False,
        schedule_interval=schedule_interval,
        default_args=default_dag_args)

    def create_task_and_add_to_dag(table_definition):

        def parse_task(ds, **kwargs):

            client = bigquery.Client()

            create_or_update_table_from_table_definition(
                bigquery_client=client,
                table_definition=table_definition,
                ds=ds,
                source_project_id=SOURCE_PROJECT_ID,
                source_dataset_name=SOURCE_DATASET_NAME,
                destination_project_id=parse_destination_dataset_project_id,
                sqls_folder=os.path.join(dags_folder, 'resources/stages/parse/sqls'),
                parse_all_partitions=parse_all_partitions
            )

        table_name = table_definition['table']['table_name']
        parsing_operator = PythonOperator(
            task_id=table_name,
            python_callable=parse_task,
            provide_context=True,
            execution_timeout=timedelta(minutes=60),
            dag=dag
        )

        ref_dependencies = ref_regex.findall(table_definition['parser']['contract_address'])
        return parsing_operator, ref_dependencies

    wait_for_ethereum_load_dag_task = ExternalTaskSensor(
        task_id='wait_for_ethereum_load_dag',
        external_dag_id='ethereum_load_dag',
        external_task_id='verify_logs_have_latest',
        execution_delta=timedelta(hours=1),
        priority_weight=0,
        mode='reschedule',
        poke_interval=5 * 60,
        timeout=60 * 60 * 12,
        dag=dag)

    files = get_list_of_json_files(dataset_folder)
    logging.info('files')
    logging.info(files)

    all_parse_tasks = {}
    task_dependencies = {}
    for f in files:
        table_definition = read_json_file(f)
        task, dependencies = create_task_and_add_to_dag(table_definition)
        wait_for_ethereum_load_dag_task >> task
        all_parse_tasks[task.task_id] = task
        task_dependencies[task.task_id] = dependencies

    for task, dependencies in task_dependencies.items():
        for dependency in dependencies:
            if dependency not in all_parse_tasks:
                raise ValueError('Table {} is not found in the the dataset. Check your ref() in contract_address field.'.format(dependency))
            all_parse_tasks[dependency] >> all_parse_tasks[task]

    if notification_emails and len(notification_emails) > 0 and send_success_email:
        send_email_task = EmailOperator(
            task_id='send_email',
            to=[email.strip() for email in notification_emails.split(',')],
            subject='Ethereum ETL Airflow Parse DAG Succeeded',
            html_content='Ethereum ETL Airflow Parse DAG Succeeded for {}'.format(dag_id),
            dag=dag
        )
        for task in all_parse_tasks.values():
            task >> send_email_task
    return dag


def get_list_of_json_files(dataset_folder):
    logging.info('get_list_of_json_files')
    logging.info(dataset_folder)
    logging.info(os.path.join(dataset_folder, '*.json'))
    return [f for f in glob(os.path.join(dataset_folder, '*.json'))]
