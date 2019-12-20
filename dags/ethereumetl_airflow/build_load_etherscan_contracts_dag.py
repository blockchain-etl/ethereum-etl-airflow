from __future__ import print_function

import json
import logging
import os
import time
from datetime import datetime, timedelta

from airflow import models
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
from google.cloud.bigquery import TimePartitioning

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)


def build_load_etherscan_contracts_dag(
    dag_id,
    output_bucket,
    destination_dataset_project_id,
    notification_emails=None,
    etherscan_load_start_date=datetime(2018, 7, 1),
    etherscan_load_all_partitions=True,
    schedule_interval='0 0 * * *',
    load_all_partitions=True
):

    dataset_name = 'etherscan'
    dataset_name_raw = 'etherscan_raw'
    dataset_name_temp = 'etherscan_temp'

    if not destination_dataset_project_id:
        raise ValueError('destination_dataset_project_id is required')

    environment = {
        'dataset_name': dataset_name,
        'dataset_name_raw': dataset_name_raw,
        'dataset_name_temp': dataset_name_temp,
        'destination_dataset_project_id': destination_dataset_project_id,
        'load_all_partitions': load_all_partitions
    }

    def read_bigquery_schema_from_file(filepath):
        result = []
        file_content = read_file(filepath)
        json_content = json.loads(file_content)
        for field in json_content:
            result.append(bigquery.SchemaField(
                name=field.get('name'),
                field_type=field.get('type', 'STRING'),
                mode=field.get('mode', 'NULLABLE'),
                description=field.get('description')))
        return result

    def read_file(filepath):
        with open(filepath) as file_handle:
            content = file_handle.read()
            return content

    def submit_bigquery_job(job, configuration):
        try:
            logging.info('Creating a job: ' + json.dumps(configuration.to_api_repr()))
            result = job.result()
            logging.info(result)
            assert job.errors is None or len(job.errors) == 0
            return result
        except Exception:
            logging.info(job.errors)
            raise

    default_dag_args = {
        'depends_on_past': False,
        'start_date': etherscan_load_start_date,
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

    def add_load_tasks(allow_quoted_newlines=False):
        wait_sensor = GoogleCloudStorageObjectSensor(
            task_id='wait_latest_contract_codes',
            timeout=60 * 60,
            poke_interval=60,
            bucket=output_bucket,
            object='export/contract_codes/block_date={datestamp}/contract_codes.csv'.format(
                datestamp='{{ds}}'),
            dag=dag
        )

        def load_task():
            client = bigquery.Client()
            job_config = bigquery.LoadJobConfig()
            schema_path = os.path.join(dags_folder, 'resources/stages/raw/schemas/contract_codes.json')
            job_config.schema = read_bigquery_schema_from_file(schema_path)
            job_config.source_format = bigquery.SourceFormat.CSV
            job_config.skip_leading_rows = 1
            job_config.write_disposition = 'WRITE_TRUNCATE'
            job_config.allow_quoted_newlines = allow_quoted_newlines
            job_config.ignore_unknown_values = True

            export_location_uri = 'gs://{bucket}/export'.format(bucket=output_bucket)
            uri = '{export_location_uri}/contract_codes/*.csv'.format(
                export_location_uri=export_location_uri)
            table_ref = client.dataset(dataset_name_raw).table('contract_codes')
            load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
            submit_bigquery_job(load_job, job_config)
            assert load_job.state == 'DONE'

        load_operator = PythonOperator(
            task_id='load_contract_codes',
            python_callable=load_task,
            execution_timeout=timedelta(minutes=30),
            dag=dag
        )

        wait_sensor >> load_operator
        return load_operator


    load_contract_codes_task = add_load_tasks()

    return dag
