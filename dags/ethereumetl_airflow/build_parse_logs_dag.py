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
from airflow.operators.sensors import ExternalTaskSensor
from google.cloud import bigquery
from google.cloud.bigquery import TimePartitioning
from glob import glob
from google.api_core.exceptions import Conflict

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

dags_folder = os.environ.get('DAGS_FOLDER', '/home/airflow/gcs/dags')

def build_parse_logs_dag(
    dag_id,
    destination_dataset_project_id,
    copy_dataset_project_id=None,
    copy_dataset_name=None,
    chain='ethereum',
    notification_emails=None,
    load_start_date=datetime(2018, 7, 1),
    schedule_interval='0 0 * * *',
    load_all_partitions=True
):

    source_dataset_name = f'crypto_{chain}'

    if not destination_dataset_project_id:
        raise ValueError('destination_dataset_project_id is required')

    environment = {
        'source_dataset_name': source_dataset_name,
        'destination_dataset_project_id': destination_dataset_project_id,
        'load_all_partitions': load_all_partitions
    }

    default_dag_args = {
        'depends_on_past': False,
        'start_date': load_start_date,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 5,
        'retry_delay': timedelta(minutes=5)
    }

    dag = models.DAG(
        dag_id,
        catchup=False,
        schedule_interval=schedule_interval,
        default_args=default_dag_args)

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

    def get_parse_logs_sql_template():
        filepath = os.path.join(dags_folder, 'resources/stages/parse/sqls/parse_logs.sql')
        with open(filepath) as file_handle:
            content = file_handle.read()
            return content

    def get_list_of_json_files():
        folder = os.path.join(dags_folder, 'resources/stages/parse/table_definitions/')
        logging.info('folder')
        logging.info(folder)
        return [f for f in glob(folder + '*.json')]

    def read_json_file(filepath):
        with open(filepath) as file_handle:
            content = file_handle.read()
            return json.loads(content)

    def create_struct_string_from_schema(schema):
        return ', '.join([f.get('name') + ' ' + f.get('type') for f in schema])

    def read_bigquery_schema_from_dict(schema):
        result = []
        for field in schema:
            result.append(bigquery.SchemaField(
                name=field.get('name'),
                field_type=field.get('type', 'STRING'),
                mode=field.get('mode', 'NULLABLE'),
                description=field.get('description')))
        return result

    def create_task_and_add_to_dag(task_config):
        dataset_name = task_config['table']['dataset_name']
        table_name = task_config['table']['table_name']
        table_description = task_config['table']['table_description']
        schema = task_config['table']['schema']
        parser = task_config['parser']
        abi = json.dumps(parser['abi'])
        columns = [c.get('name') for c in schema if c.get('name') != 'block_timestamp']
        
        def parse_task(ds, **kwargs):
            template_context = kwargs.copy()
            template_context['ds'] = ds
            template_context['params'] = environment
            template_context['columns'] = columns
            template_context['parser'] = parser
            template_context['abi'] = abi
            template_context['struct_fields'] = create_struct_string_from_schema(schema)
            client = bigquery.Client()
            dataset = client.dataset(dataset_name)
            #try:
                #dataset = client.create_dataset(dataset)
            #except Conflict as error:
                #print('Dataset already exists')
            table_ref = dataset.table(table_name)
            job_config = bigquery.QueryJobConfig()
            job_config.priority = bigquery.QueryPriority.INTERACTIVE
            job_config.destination = table_ref
            job_config.write_disposition = 'WRITE_TRUNCATE'
            job_config.schema = read_bigquery_schema_from_dict(schema)
            sql_template = get_parse_logs_sql_template()
            sql = kwargs['task'].render_template('', sql_template, template_context)
            logging.info('sql')
            logging.info(sql)
            query_job = client.query(sql, location='US', job_config=job_config)
            submit_bigquery_job(query_job, job_config)
            assert query_job.state == 'DONE'
        
        # TODO: remove - only here for testing
        #parse_task('2019-10-01', **{'task': PythonOperator(python_callable=print, task_id='dummy', dag=dag)})
        
        parsing_operator = PythonOperator(
            task_id='parse_logs_{dataset_name}_{table_name}'.format(
                dataset_name=dataset_name,
                table_name=table_name
                ),
            python_callable=parse_task,
            provide_context=True,
            execution_timeout=timedelta(minutes=60),
            dag=dag
        )
        return parsing_operator

    wait_for_ethereum_load_dag_task = ExternalTaskSensor(
        task_id='wait_for_ethereum_load_dag',
        external_dag_id='ethereum_load_dag',
        external_task_id='verify_logs_have_latest',
        dag=dag)

    files = get_list_of_json_files()
    logging.info('files')
    logging.info(files)

    for f in files:
        task_config = read_json_file(f)
        task = create_task_and_add_to_dag(task_config)
        wait_for_ethereum_load_dag_task >> task
    return dag

# TODO: remove, only here for testing
#build_parse_logs_dag('ethereum_parse_logs_dag', '42')