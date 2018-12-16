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


# The following datasets must be created in BigQuery:
# - ethereum_blockchain_raw
# - ethereum_blockchain_temp
# - ethereum_blockchain
# Environment variable OUTPUT_BUCKET must be set and point to the GCS bucket
# where files exported by export_dag.py are located

dataset_name = os.environ.get('DATASET_NAME', 'ethereum_blockchain')
dataset_name_raw = os.environ.get('DATASET_NAME_RAW', 'ethereum_blockchain_raw')
dataset_name_temp = os.environ.get('DATASET_NAME_TEMP', 'ethereum_blockchain_temp')
destination_dataset_project_id = os.environ.get('DESTINATION_DATASET_PROJECT_ID')
if not destination_dataset_project_id:
    raise ValueError('DESTINATION_DATASET_PROJECT_ID is required')

environment = {
    'DATASET_NAME': dataset_name,
    'DATASET_NAME_RAW': dataset_name_raw,
    'DATASET_NAME_TEMP': dataset_name_temp,
    'DESTINATION_DATASET_PROJECT_ID': destination_dataset_project_id
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
        for key, value in environment.items():
            # each bracket should be doubled to be escaped
            # we need two escaped and one unescaped
            content = content.replace('{{{{{key}}}}}'.format(key=key), value)
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
    'start_date': datetime(2018, 7, 1),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

notification_emails = os.environ.get('NOTIFICATION_EMAILS')
if notification_emails and len(notification_emails) > 0:
    default_dag_args['email'] = [email.strip() for email in notification_emails.split(',')]

# Define a DAG (directed acyclic graph) of tasks.
dag = models.DAG(
    'ethereumetl_load_dag',
    catchup=False,
    # Daily at 1:30am
    schedule_interval='30 1 * * *',
    default_args=default_dag_args)

dags_folder = os.environ.get('DAGS_FOLDER', '/home/airflow/gcs/dags')


def add_load_tasks(task, file_format, allow_quoted_newlines=False):
    output_bucket = os.environ.get('OUTPUT_BUCKET')
    if output_bucket is None:
        raise ValueError('You must set OUTPUT_BUCKET environment variable')

    wait_sensor = GoogleCloudStorageObjectSensor(
        task_id='wait_latest_{task}'.format(task=task),
        timeout=60 * 60,
        poke_interval=60,
        bucket=output_bucket,
        object='export/{task}/block_date={datestamp}/{task}.{file_format}'.format(
            task=task, datestamp='{{ds}}', file_format=file_format),
        dag=dag
    )

    def load_task():
        client = bigquery.Client()
        job_config = bigquery.LoadJobConfig()
        schema_path = os.path.join(dags_folder, 'resources/stages/raw/schemas/{task}.json'.format(task=task))
        job_config.schema = read_bigquery_schema_from_file(schema_path)
        job_config.source_format = bigquery.SourceFormat.CSV if file_format == 'csv' else bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        if file_format == 'csv':
            job_config.skip_leading_rows = 1
        job_config.write_disposition = 'WRITE_TRUNCATE'
        job_config.allow_quoted_newlines = allow_quoted_newlines

        export_location_uri = 'gs://{bucket}/export'.format(bucket=output_bucket)
        uri = '{export_location_uri}/{task}/*.{file_format}'.format(
            export_location_uri=export_location_uri, task=task, file_format=file_format)
        table_ref = client.dataset(dataset_name_raw).table(task)
        load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
        submit_bigquery_job(load_job, job_config)
        assert load_job.state == 'DONE'

    load_operator = PythonOperator(
        task_id='load_{task}'.format(task=task),
        python_callable=load_task,
        execution_timeout=timedelta(minutes=30),
        dag=dag
    )

    wait_sensor >> load_operator
    return load_operator


def add_enrich_tasks(task, time_partitioning_field='block_timestamp', dependencies=None):
    def enrich_task():
        client = bigquery.Client()

        # Need to use a temporary table because bq query sets field modes to NULLABLE and descriptions to null
        # when writeDisposition is WRITE_TRUNCATE

        # Create a temporary table
        temp_table_name = '{task}_{milliseconds}'.format(task=task, milliseconds=int(round(time.time() * 1000)))
        temp_table_ref = client.dataset(dataset_name_temp).table(temp_table_name)

        schema_path = os.path.join(dags_folder, 'resources/stages/enrich/schemas/{task}.json'.format(task=task))
        schema = read_bigquery_schema_from_file(schema_path)
        table = bigquery.Table(temp_table_ref, schema=schema)

        description_path = os.path.join(
            dags_folder, 'resources/stages/enrich/descriptions/{task}.txt'.format(task=task))
        table.description = read_file(description_path)
        if time_partitioning_field is not None:
            table.time_partitioning = TimePartitioning(field=time_partitioning_field)
        logging.info('Creating table: ' + json.dumps(table.to_api_repr()))
        table = client.create_table(table)
        assert table.table_id == temp_table_name

        # Query from raw to temporary table
        query_job_config = bigquery.QueryJobConfig()
        # Finishes faster, query limit for concurrent interactive queries is 50
        query_job_config.priority = bigquery.QueryPriority.INTERACTIVE
        query_job_config.destination = temp_table_ref
        sql_path = os.path.join(dags_folder, 'resources/stages/enrich/sqls/{task}.sql'.format(task=task))
        sql = read_file(sql_path)
        query_job = client.query(sql, location='US', job_config=query_job_config)
        submit_bigquery_job(query_job, query_job_config)
        assert query_job.state == 'DONE'

        # Copy temporary table to destination
        copy_job_config = bigquery.CopyJobConfig()
        copy_job_config.write_disposition = 'WRITE_TRUNCATE'

        dest_table_name = '{task}'.format(task=task)
        project = os.environ.get('DESTINATION_DATASET_PROJECT_ID', None)
        dest_table_ref = client.dataset(dataset_name, project=project).table(dest_table_name)
        copy_job = client.copy_table(temp_table_ref, dest_table_ref, location='US', job_config=copy_job_config)
        submit_bigquery_job(copy_job, copy_job_config)
        assert copy_job.state == 'DONE'

        # Delete temp table
        client.delete_table(temp_table_ref)

    enrich_operator = PythonOperator(
        task_id='enrich_{task}'.format(task=task),
        python_callable=enrich_task,
        execution_timeout=timedelta(minutes=60),
        dag=dag
    )

    if dependencies is not None and len(dependencies) > 0:
        for dependency in dependencies:
            dependency >> enrich_operator
    return enrich_operator


def add_verify_tasks(task, dependencies=None):
    # The queries in verify/sqls will fail when the condition is not met
    # Have to use this trick since the Python 2 version of BigQueryCheckOperator doesn't support standard SQL
    # and legacy SQL can't be used to query partitioned tables.
    sql_path = os.path.join(dags_folder, 'resources/stages/verify/sqls/{task}.sql'.format(task=task))
    sql = read_file(sql_path)
    verify_task = BigQueryOperator(
        task_id='verify_{task}'.format(task=task),
        bql=sql,
        use_legacy_sql=False,
        dag=dag)
    if dependencies is not None and len(dependencies) > 0:
        for dependency in dependencies:
            dependency >> verify_task
    return verify_task


load_blocks_task = add_load_tasks('blocks', 'csv')
load_transactions_task = add_load_tasks('transactions', 'csv')
load_receipts_task = add_load_tasks('receipts', 'csv')
load_logs_task = add_load_tasks('logs', 'json')
load_contracts_task = add_load_tasks('contracts', 'json')
load_tokens_task = add_load_tasks('tokens', 'csv', allow_quoted_newlines=True)
load_token_transfers_task = add_load_tasks('token_transfers', 'csv')
load_traces_task = add_load_tasks('traces', 'csv')

enrich_blocks_task = add_enrich_tasks(
    'blocks', time_partitioning_field='timestamp', dependencies=[load_blocks_task])
enrich_transactions_task = add_enrich_tasks(
    'transactions', dependencies=[load_blocks_task, load_transactions_task, load_receipts_task])
enrich_logs_task = add_enrich_tasks(
    'logs', dependencies=[load_blocks_task, load_logs_task])
enrich_contracts_task = add_enrich_tasks(
    'contracts', dependencies=[load_blocks_task, load_contracts_task])
enrich_tokens_task = add_enrich_tasks(
    'tokens', time_partitioning_field=None, dependencies=[load_tokens_task])
enrich_token_transfers_task = add_enrich_tasks(
    'token_transfers', dependencies=[load_blocks_task, load_token_transfers_task])
enrich_traces_task = add_enrich_tasks(
    'traces', dependencies=[load_blocks_task, load_traces_task])

verify_blocks_count_task = add_verify_tasks('blocks_count', [enrich_blocks_task])
verify_blocks_have_latest_task = add_verify_tasks('blocks_have_latest', [enrich_blocks_task])
verify_transactions_count_task = add_verify_tasks('transactions_count', [enrich_blocks_task, enrich_transactions_task])
verify_transactions_have_latest_task = add_verify_tasks('transactions_have_latest', [enrich_transactions_task])
verify_logs_have_latest_task = add_verify_tasks('logs_have_latest', [enrich_logs_task])
verify_token_transfers_have_latest_task = add_verify_tasks('token_transfers_have_latest', [enrich_token_transfers_task])
verify_traces_blocks_count_task = add_verify_tasks(
    'traces_blocks_count', [enrich_blocks_task, enrich_traces_task])
verify_traces_transactions_count_task = add_verify_tasks(
    'traces_transactions_count', [enrich_transactions_task, enrich_traces_task])
verify_traces_contracts_count_task = add_verify_tasks(
    'traces_contracts_count', [enrich_transactions_task, enrich_traces_task])

if notification_emails and len(notification_emails) > 0:
    send_email_task = EmailOperator(
        task_id='send_email',
        to=[email.strip() for email in notification_emails.split(',')],
        subject='Ethereum ETL Airflow Load DAG Succeeded',
        html_content='Ethereum ETL Airflow Load DAG Succeeded',
        dag=dag
    )
    verify_blocks_count_task >> send_email_task
    verify_blocks_have_latest_task >> send_email_task
    verify_transactions_count_task >> send_email_task
    verify_transactions_have_latest_task >> send_email_task
    verify_logs_have_latest_task >> send_email_task
    verify_token_transfers_have_latest_task >> send_email_task
    verify_traces_blocks_count_task >> send_email_task
    verify_traces_transactions_count_task >> send_email_task
    verify_traces_contracts_count_task >> send_email_task
