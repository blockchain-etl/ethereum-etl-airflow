from __future__ import print_function

import logging
import os
from datetime import datetime, timedelta

from airflow import models
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

    table_partition_keys = {
	'blocks': 'number',
	'contracts': 'address',
	'logs': 'block_number',
	'receipts': 'block_number',
	'token_transfers': 'block_number',
	'tokens': 'address',
	'traces': 'block_number',
	'transactions': 'block_number'
    }

    sql = """
	DROP TABLE IF EXISTS {schema}.{table}_copy_tmp;

	CREATE TABLE {schema}.{table}_copy_tmp
	(LIKE {schema}.{table});
    """

    if file_format == 'csv':
	sql += """
	    COPY {schema}.{table}_copy_tmp
	    FROM 's3://{output_bucket}/export/{table}/block_date={date}/{table}.{file_format}'
	    WITH CREDENTIALS
	    'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}'
	    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL IGNOREHEADER 1 CSV;
	"""
    elif file_format == 'json':
	sql += """
	    COPY {schema}.{table}_copy_tmp
	    FROM 's3://{output_bucket}/export/{table}/block_date={date}/{table}.{file_format}'
	    WITH CREDENTIALS
	    'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}'
	    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL JSON 'auto';
	"""
    else:
	raise ValueError('Only json and csv file formats are supported.')

    sql += """
	BEGIN TRANSACTION;

	DELETE FROM {schema}.{table}
	USING {schema}.{table}_copy_tmp
	WHERE
	  {schema}.{table}.{partition_key} = {schema}.{table}_copy_tmp.{partition_key};

	INSERT INTO {schema}.{table}
	SELECT * FROM {schema}.{table}_copy_tmp;

	END TRANSACTION;

	DROP TABLE {schema}.{table}_copy_tmp;
    """

    formatted_sql = sql.format(
	schema='ethereum',
	table=task,
	partition_key=table_partition_keys[task],
	output_bucket=output_bucket,
	date=ds,
	file_format=file_format,
	aws_access_key_id=aws_access_key_id,
	aws_secret_access_key=aws_secret_access_key
    )
    pg_hook.run(formatted_sql)


def add_load_tasks(task, file_format):

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

    return load_operator


load_blocks_task = add_load_tasks('blocks', 'csv')
load_transactions_task = add_load_tasks('transactions', 'csv')
load_receipts_task = add_load_tasks('receipts', 'csv')
load_logs_task = add_load_tasks('logs', 'json')
load_contracts_task = add_load_tasks('contracts', 'json')
load_tokens_task = add_load_tasks('tokens', 'csv')
load_token_transfers_task = add_load_tasks('token_transfers', 'csv')
