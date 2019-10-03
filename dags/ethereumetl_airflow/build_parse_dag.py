from __future__ import print_function

import json
import logging
import os
import time
from datetime import datetime, timedelta
from glob import glob

from airflow import models
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import ExternalTaskSensor
from google.api_core.exceptions import Conflict
from google.cloud import bigquery
from eth_utils import event_abi_to_log_topic, function_abi_to_4byte_selector
from google.cloud.bigquery import TimePartitioning

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

dags_folder = os.environ.get('DAGS_FOLDER', '/home/airflow/gcs/dags')


def build_parse_dag(
        dag_id,
        dataset_folder,
        parse_destination_dataset_project_id,
        load_start_date=datetime(2018, 7, 1),
        schedule_interval='0 0 * * *',
        enabled=True,
        parse_all_partitions=True
):
    if not enabled:
        logging.info('enabled is False, the DAG will not be built.')
        return None

    logging.info('parse_all_partitions is {}'.format(parse_all_partitions))

    SOURCE_PROJECT_ID = 'bigquery-public-data'
    SOURCE_DATASET_NAME = 'crypto_ethereum'

    environment = {
        'source_project_id': SOURCE_PROJECT_ID,
        'source_dataset_name': SOURCE_DATASET_NAME
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

    def create_task_and_add_to_dag(task_config):
        dataset_name = 'ethereum_' + task_config['table']['dataset_name']
        table_name = task_config['table']['table_name']
        table_description = task_config['table']['table_description']
        schema = task_config['table']['schema']
        parser = task_config['parser']
        parser_type = parser.get('type', 'log')
        abi = json.dumps(parser['abi'])
        columns = [c.get('name') for c in schema]

        def parse_task(ds, **kwargs):
            template_context = kwargs.copy()
            template_context['ds'] = ds
            template_context['params'] = environment
            template_context['params']['table_name'] = table_name
            template_context['params']['columns'] = columns
            template_context['params']['parser'] = parser
            template_context['params']['abi'] = abi
            if parser_type == 'log':
                template_context['params']['event_topic'] = abi_to_event_topic(parser['abi'])
            elif parser_type == 'trace':
                template_context['params']['method_selector'] = abi_to_method_selector(parser['abi'])
            template_context['params']['struct_fields'] = create_struct_string_from_schema(schema)
            template_context['params']['parse_all_partitions'] = parse_all_partitions
            client = bigquery.Client()

            # # # Create a temporary table

            dataset_name_temp = 'parse_temp'
            create_dataset(client, dataset_name_temp)
            temp_table_name = 'temp_{table_name}_{milliseconds}'\
                .format(table_name=table_name, milliseconds=int(round(time.time() * 1000)))
            temp_table_ref = client.dataset(dataset_name_temp).table(temp_table_name)

            temp_table = bigquery.Table(temp_table_ref, schema=read_bigquery_schema_from_dict(schema, parser_type))

            temp_table.description = table_description
            temp_table.time_partitioning = TimePartitioning(field='block_timestamp')
            logging.info('Creating table: ' + json.dumps(temp_table.to_api_repr()))
            temp_table = client.create_table(temp_table)
            assert temp_table.table_id == temp_table_name

            # # # Query to temporary table

            job_config = bigquery.QueryJobConfig()
            job_config.priority = bigquery.QueryPriority.INTERACTIVE
            job_config.destination = temp_table_ref
            sql_template = get_parse_sql_template(parser_type)
            sql = kwargs['task'].render_template('', sql_template, template_context)
            logging.info(sql)
            query_job = client.query(sql, location='US', job_config=job_config)
            submit_bigquery_job(query_job, job_config)
            assert query_job.state == 'DONE'

            # # # Copy / merge to destination

            if parse_all_partitions:
                # Copy temporary table to destination
                copy_job_config = bigquery.CopyJobConfig()
                copy_job_config.write_disposition = 'WRITE_TRUNCATE'
                dest_table_ref = client.dataset(dataset_name, project=parse_destination_dataset_project_id).table(table_name)
                copy_job = client.copy_table(temp_table_ref, dest_table_ref, location='US', job_config=copy_job_config)
                submit_bigquery_job(copy_job, copy_job_config)
                assert copy_job.state == 'DONE'
            else:
                # Merge
                # https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement
                merge_job_config = bigquery.QueryJobConfig()
                # Finishes faster, query limit for concurrent interactive queries is 50
                merge_job_config.priority = bigquery.QueryPriority.INTERACTIVE

                merge_sql_template = get_merge_table_sql_template()
                merge_template_context = template_context.copy()
                merge_template_context['params']['source_table'] = temp_table_name
                merge_template_context['params']['destination_dataset_project_id'] = parse_destination_dataset_project_id
                merge_template_context['params']['destination_dataset_name'] = dataset_name
                merge_template_context['params']['dataset_name_temp'] = dataset_name_temp
                merge_template_context['params']['columns'] = columns
                merge_sql = kwargs['task'].render_template('', merge_sql_template, merge_template_context)
                print('Merge sql:')
                print(merge_sql)
                merge_job = client.query(merge_sql, location='US', job_config=merge_job_config)
                submit_bigquery_job(merge_job, merge_job_config)
                assert merge_job.state == 'DONE'

            # Delete temp table
            client.delete_table(temp_table_ref)

        parsing_operator = PythonOperator(
            task_id=table_name,
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

    files = get_list_of_json_files(dataset_folder)
    logging.info('files')
    logging.info(files)

    for f in files:
        task_config = read_json_file(f)
        task = create_task_and_add_to_dag(task_config)
        wait_for_ethereum_load_dag_task >> task
    return dag


def abi_to_event_topic(abi):
    return '0x' + event_abi_to_log_topic(abi).hex()


def abi_to_method_selector(abi):
    return '0x' + function_abi_to_4byte_selector(abi).hex()

def get_list_of_json_files(dataset_folder):
    logging.info('get_list_of_json_files')
    logging.info(dataset_folder)
    logging.info(os.path.join(dataset_folder, '*.json'))
    return [f for f in glob(os.path.join(dataset_folder, '*.json'))]


def get_parse_sql_template(parser_type):
    return get_parse_logs_sql_template() if parser_type == 'log' else get_parse_traces_sql_template()


def get_parse_logs_sql_template():
    filepath = os.path.join(dags_folder, 'resources/stages/parse/sqls/parse_logs.sql')
    with open(filepath) as file_handle:
        content = file_handle.read()
        return content


def get_parse_traces_sql_template():
    filepath = os.path.join(dags_folder, 'resources/stages/parse/sqls/parse_traces.sql')
    with open(filepath) as file_handle:
        content = file_handle.read()
        return content


def get_merge_table_sql_template():
    filepath = os.path.join(dags_folder, 'resources/stages/parse/sqls/merge_table.sql')
    with open(filepath) as file_handle:
        content = file_handle.read()
        return content


def read_json_file(filepath):
    with open(filepath) as file_handle:
        content = file_handle.read()
        return json.loads(content)


def create_struct_string_from_schema(schema):
    return ', '.join(['`' + f.get('name') + '` ' + f.get('type') for f in schema])


def read_bigquery_schema_from_dict(schema, parser_type):
    result = [
        bigquery.SchemaField(
            name='block_timestamp',
            field_type='TIMESTAMP',
            mode='REQUIRED',
            description='Timestamp of the block where this event was emitted'),
        bigquery.SchemaField(
            name='block_number',
            field_type='INTEGER',
            mode='REQUIRED',
            description='The block number where this event was emitted'),
        bigquery.SchemaField(
            name='transaction_hash',
            field_type='STRING',
            mode='REQUIRED',
            description='Hash of the transactions in which this event was emitted')
    ]
    if parser_type == 'log':
        result.append(bigquery.SchemaField(
            name='log_index',
            field_type='INTEGER',
            mode='REQUIRED',
            description='Integer of the log index position in the block of this event'))
    elif parser_type == 'trace':
        result.append(bigquery.SchemaField(
            name='trace_address',
            field_type='STRING',
            description='Comma separated list of trace address in call tree'))
    for field in schema:
        result.append(bigquery.SchemaField(
            name=field.get('name'),
            field_type=field.get('type', 'STRING'),
            mode=field.get('mode', 'NULLABLE'),
            description=field.get('description')))
    return result


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


def create_dataset(client, dataset_name):
    dataset = client.dataset(dataset_name)
    try:
        logging.info('Creating new dataset ...')
        dataset = client.create_dataset(dataset)
        logging.info('New dataset created: ' + dataset_name)
    except Conflict as error:
        logging.info('Dataset already exists')

    return dataset
