from __future__ import print_function

import json
import logging
import os
import time
from datetime import datetime, timedelta

from airflow import models
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer
from airflow.sensors.s3_key_sensor import S3KeySensor

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)


default_dag_args = {
    'depends_on_past': False,
    'start_date': datetime(2015, 7, 30),
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
    dag_id='ethereumetl_load_dag_redshift',
    # Daily at 1:30am
    schedule_interval='30 1 * * *',
    default_args=default_dag_args)

dags_folder = os.environ.get('DAGS_FOLDER', '/usr/local/airflow/dags/ethereum-etl-airflow/dags')


# Check for required env vars
output_bucket = os.environ.get('OUTPUT_BUCKET')
if output_bucket is None:
    raise ValueError('You must set OUTPUT_BUCKET environment variable')

aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
if aws_access_key_id is None:
    raise ValueError('You must set AWS_ACCESS_KEY_ID environment variable')

aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
if aws_secret_access_key is None:
    raise ValueError('You must set AWS_SECRET_ACCESS_KEY environment variable')


def load_task(ds, **kwargs):
    conn_id = kwargs.get('conn_id')
    file_format = kwargs.get('file_format')
    task = kwargs.get('task')
    pg_hook = PostgresHook(conn_id)

    if file_format == 'csv':
	sql = """
	    COPY {schema}.{table}
	    FROM 's3://{output_bucket}/export/{table}/block_date={date}/{table}.{file_format}'
	    WITH CREDENTIALS
	    'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}'
	    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL IGNOREHEADER 1 CSV;
	"""
    elif file_format == 'json':
	sql = """
	    COPY {schema}.{table}
	    FROM 's3://{output_bucket}/export/{table}/block_date={date}/{table}.{file_format}'
	    WITH CREDENTIALS
	    'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}'
	    json 'auto';
	"""
    else:
	raise ValueError('Only json and csv file formats are supported.')

    formatted_sql = sql.format(
	schema='ethereum',
	table=task,
	output_bucket=output_bucket,
	date=ds,
	file_format=file_format,
	aws_access_key_id=aws_access_key_id,
	aws_secret_access_key=aws_secret_access_key
    )
    result = pg_hook.run(formatted_sql)
    print(str(result))
    # assert


def add_load_tasks(task, file_format, allow_quoted_newlines=False):

    #wait_sensor = S3KeySensor(
    #    task_id='wait_latest_{task}'.format(task=task),
    #    dag=dag,
    #    bucket_name=output_bucket,
    #    bucket_key='export/{task}/block_date={datestamp}/{task}.{file_format}'.format(
    #        task=task, datestamp='{{ds}}', file_format=file_format),
    #    wildcard_match=False,
    #    aws_conn_id='aws_default',
    #    verify=None,
    #    timeout=60 * 60,
    #    poke_interval=60
    #)

    load_operator = PythonOperator(
	task_id='s3_to_redshift_{task}'.format(task=task),
	dag = dag,
	python_callable=load_task,
	provide_context=True,
	op_kwargs={
	    'conn_id'    : 'redshift',
	    'file_format': file_format,
	    'task'       : task
	},
    )

    #wait_sensor >> load_operator
    return load_operator


load_blocks_task = add_load_tasks('blocks', 'csv')
load_transactions_task = add_load_tasks('transactions', 'csv')
load_receipts_task = add_load_tasks('receipts', 'csv')
load_logs_task = add_load_tasks('logs', 'json')
load_contracts_task = add_load_tasks('contracts', 'json')
load_tokens_task = add_load_tasks('tokens', 'csv', allow_quoted_newlines=True)
load_token_transfers_task = add_load_tasks('token_transfers', 'csv')
load_traces_task = add_load_tasks('traces', 'csv')
