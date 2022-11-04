from __future__ import print_function

import collections
import logging
import os
from datetime import datetime, timedelta

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from google.cloud import bigquery

from ethereumetl_airflow.bigquery_utils import create_view, share_dataset_all_users_read
from ethereumetl_airflow.common import read_json_file, read_file, get_list_of_files
from ethereumetl_airflow.parse.parse_dataset_folder_logic import parse_dataset_folder
from ethereumetl_airflow.parse.parse_table_definition_logic import create_dataset

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
        parse_all_partitions=None
):

    logging.info('parse_all_partitions is {}'.format(parse_all_partitions))

    if 'ethereum_kovan_parse' in dag_id:
        SOURCE_PROJECT_ID = 'public-data-finance'
        SOURCE_DATASET_NAME = 'crypto_ethereum_kovan'

        PARTITION_DAG_ID = 'ethereum_kovan_partition_dag'
    else:
        SOURCE_PROJECT_ID = 'bigquery-public-data'
        SOURCE_DATASET_NAME = 'crypto_ethereum'

        PARTITION_DAG_ID = 'ethereum_partition_dag'

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

    def create_parse_task():

        def parse_task(ds, **kwargs):
            validate_definition_files(dataset_folder)

            client = bigquery.Client()

            parse_dataset_folder(
                bigquery_client=client,
                dataset_folder=dataset_folder,
                ds=ds,
                source_project_id=SOURCE_PROJECT_ID,
                source_dataset_name=SOURCE_DATASET_NAME,
                destination_project_id=parse_destination_dataset_project_id,
                sqls_folder=os.path.join(dags_folder, 'resources/stages/parse/sqls'),
                parse_all_partitions=parse_all_partitions
            )

        dataset_name = get_dataset_name(dataset_folder)
        parsing_operator = PythonOperator(
            task_id=f'parse_tables_{dataset_name}',
            python_callable=parse_task,
            execution_timeout=timedelta(minutes=60 * 4),
            dag=dag
        )

        return parsing_operator

    def create_add_view_task(dataset_name, view_name, sql):
        def create_view_task(ds, **kwargs):
            client = bigquery.Client()

            dest_table_name = view_name
            dest_table_ref = create_dataset(client, dataset_name, parse_destination_dataset_project_id).table(dest_table_name)

            print('View sql: \n' + sql)

            create_view(client, sql, dest_table_ref)

        create_view_operator = PythonOperator(
            task_id=f'create_view_{view_name}',
            python_callable=create_view_task,
            execution_timeout=timedelta(minutes=10),
            dag=dag
        )

        return create_view_operator

    def create_share_dataset_task(dataset_name):
        def share_dataset_task(**kwargs):
            if parse_destination_dataset_project_id != 'blockchain-etl':
                logging.info('Skipping sharing dataset.')
            else:
                client = bigquery.Client()
                share_dataset_all_users_read(client, f'{parse_destination_dataset_project_id}.{dataset_name}')
                share_dataset_all_users_read(client, f'{parse_destination_dataset_project_id}-internal.{dataset_name}')

        share_dataset_operator = PythonOperator(
            task_id='share_dataset',
            python_callable=share_dataset_task,
            execution_timeout=timedelta(minutes=10),
            dag=dag
        )

        return share_dataset_operator

    wait_for_ethereum_load_dag_task = ExternalTaskSensor(
        task_id='wait_for_ethereum_partition_dag',
        external_dag_id=PARTITION_DAG_ID,
        external_task_id='done',
        execution_delta=timedelta(minutes=30),
        priority_weight=0,
        mode='reschedule',
        poke_interval=5 * 60,
        timeout=60 * 60 * 12,
        dag=dag)

    parse_task = create_parse_task()
    wait_for_ethereum_load_dag_task >> parse_task

    checkpoint_task = BashOperator(
        task_id='parse_all_checkpoint',
        bash_command='echo parse_all_checkpoint',
        dag=dag
    )

    parse_task >> checkpoint_task

    dataset_name = os.path.basename(dataset_folder)
    full_dataset_name = 'ethereum_' + dataset_name

    share_dataset_task = create_share_dataset_task(full_dataset_name)
    checkpoint_task >> share_dataset_task

    # Create views

    sql_files = get_list_of_files(dataset_folder, '*.sql')
    logging.info(sql_files)

    for sql_file in sql_files:
        sql = read_file(sql_file)
        base_name = os.path.basename(sql_file)
        view_name = os.path.splitext(base_name)[0]
        create_view_task = create_add_view_task(full_dataset_name, view_name, sql)
        checkpoint_task >> create_view_task

    return dag


def validate_definition_files(dataset_folder):
    json_files = get_list_of_files(dataset_folder, '*.json')
    dataset_folder_name = get_dataset_name(dataset_folder)

    all_lowercase_table_names = []
    for json_file in json_files:
        file_name = json_file.split('/')[-1].replace('.json', '')

        table_definition = read_json_file(json_file)
        table = table_definition.get('table')
        if not table:
            raise ValueError(f'table is empty in file {json_file}')

        dataset_name = table.get('dataset_name')
        if not dataset_name:
            raise ValueError(f'dataset_name is empty in file {json_file}')
        if dataset_folder_name != dataset_name:
            raise ValueError(f'dataset_name {dataset_name} is not equal to dataset_folder_name {dataset_folder_name}')

        table_name = table.get('table_name')
        if not table_name:
            raise ValueError(f'table_name is empty in file {json_file}')
        if file_name != table_name:
            raise ValueError(f'file_name {file_name} doest match the table_name {table_name}')
        all_lowercase_table_names.append(table_name.lower())

    table_name_counts = collections.defaultdict(lambda: 0)
    for table_name in all_lowercase_table_names:
        table_name_counts[table_name] += 1

    non_unique_table_names = [name for name, count in table_name_counts.items() if count > 1]

    if len(non_unique_table_names) > 0:
        raise ValueError(f'The following table names are not unique {",".join(non_unique_table_names)}')


def get_dataset_name(dataset_folder):
    return dataset_folder.split('/')[-1]
