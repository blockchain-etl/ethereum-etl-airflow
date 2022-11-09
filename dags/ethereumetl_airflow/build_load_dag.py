from __future__ import print_function

import json
import logging
import os
import time
from datetime import datetime, timedelta
from tempfile import TemporaryDirectory

from airflow import models
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from google.cloud.bigquery import TimePartitioning, SchemaField

from ethereumetl_airflow.bigquery_utils import submit_bigquery_job
from ethereumetl_airflow.build_export_dag import upload_to_gcs

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)


def build_load_dag(
    dag_id,
    output_bucket,
    destination_dataset_project_id,
    chain='ethereum',
    notification_emails=None,
    load_start_date=datetime(2018, 7, 1),
    schedule_interval='0 0 * * *',
    load_all_partitions=True
):
    # The following datasets must be created in BigQuery:
    # - crypto_{chain}_raw
    # - crypto_{chain}_temp
    # - crypto_{chain}

    dataset_name = f'crypto_{chain}'
    dataset_name_raw = f'crypto_{chain}_raw'
    dataset_name_temp = f'crypto_{chain}_temp'

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

    def add_load_tasks(task, file_format, allow_quoted_newlines=False):
        wait_sensor = GCSObjectExistenceSensor(
            task_id='wait_latest_{task}'.format(task=task),
            timeout=60 * 60,
            poke_interval=60,
            bucket=output_bucket,
            object='export/{task}/block_date={datestamp}/{task}.{file_format}'.format(
                task=task, datestamp='{{ds}}', file_format=file_format),
            dag=dag
        )

        def load_task(ds, **kwargs):
            client = bigquery.Client()
            job_config = bigquery.LoadJobConfig()
            schema_path = os.path.join(dags_folder, 'resources/stages/raw/schemas/{task}.json'.format(task=task))
            schema = read_bigquery_schema_from_file(schema_path)
            schema = adjust_schema_for_kovan(dag_id, task, schema)
            job_config.schema = schema
            job_config.source_format = bigquery.SourceFormat.CSV if file_format == 'csv' else bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            if file_format == 'csv':
                job_config.skip_leading_rows = 1
            job_config.write_disposition = 'WRITE_TRUNCATE'
            job_config.allow_quoted_newlines = allow_quoted_newlines
            job_config.ignore_unknown_values = True

            export_location_uri = 'gs://{bucket}/export'.format(bucket=output_bucket)
            if load_all_partitions:
                uri = '{export_location_uri}/{task}/*.{file_format}'.format(
                    export_location_uri=export_location_uri, task=task, file_format=file_format)
            else:
                uri = '{export_location_uri}/{task}/block_date={ds}/*.{file_format}'.format(
                    export_location_uri=export_location_uri, task=task, ds=ds, file_format=file_format)
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

    def add_enrich_tasks(task, time_partitioning_field='block_timestamp', dependencies=None, always_load_all_partitions=False):
        def enrich_task(ds, **kwargs):
            template_context = kwargs.copy()
            template_context['ds'] = ds
            template_context['params'] = environment

            client = bigquery.Client()

            # Need to use a temporary table because bq query sets field modes to NULLABLE and descriptions to null
            # when writeDisposition is WRITE_TRUNCATE

            # Create a temporary table
            temp_table_name = '{task}_{milliseconds}'.format(task=task, milliseconds=int(round(time.time() * 1000)))
            temp_table_ref = client.dataset(dataset_name_temp).table(temp_table_name)

            schema_path = os.path.join(dags_folder, 'resources/stages/enrich/schemas/{task}.json'.format(task=task))
            schema = read_bigquery_schema_from_file(schema_path)
            schema = adjust_schema_for_kovan(dag_id, task, schema)
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
            sql_template = read_file(sql_path)
            sql = kwargs['task'].render_template(sql_template, template_context)
            print('Enrichment sql:')
            print(sql)

            query_job = client.query(sql, location='US', job_config=query_job_config)
            submit_bigquery_job(query_job, query_job_config)
            assert query_job.state == 'DONE'

            if load_all_partitions or always_load_all_partitions:
                # Copy temporary table to destination
                copy_job_config = bigquery.CopyJobConfig()
                copy_job_config.write_disposition = 'WRITE_TRUNCATE'
                dest_table_name = '{task}'.format(task=task)
                dest_table_ref = client.dataset(dataset_name, project=destination_dataset_project_id).table(dest_table_name)
                copy_job = client.copy_table(temp_table_ref, dest_table_ref, location='US', job_config=copy_job_config)
                submit_bigquery_job(copy_job, copy_job_config)
                assert copy_job.state == 'DONE'
            else:
                # Merge
                # https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement
                merge_job_config = bigquery.QueryJobConfig()
                # Finishes faster, query limit for concurrent interactive queries is 50
                merge_job_config.priority = bigquery.QueryPriority.INTERACTIVE

                merge_sql_path = os.path.join(
                    dags_folder, 'resources/stages/enrich/sqls/merge/merge_{task}.sql'.format(task=task))
                merge_sql_template = read_file(merge_sql_path)

                merge_template_context = template_context.copy()
                merge_template_context['params']['source_table'] = temp_table_name
                merge_template_context['params']['destination_dataset_project_id'] = destination_dataset_project_id
                merge_template_context['params']['destination_dataset_name'] = dataset_name
                merge_sql = kwargs['task'].render_template(merge_sql_template, merge_template_context)
                print('Merge sql:')
                print(merge_sql)
                merge_job = client.query(merge_sql, location='US', job_config=merge_job_config)
                submit_bigquery_job(merge_job, merge_job_config)
                assert merge_job.state == 'DONE'

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
        verify_task = BigQueryInsertJobOperator(
            task_id='verify_{task}'.format(task=task),
            configuration={"query": {"query": sql, "useLegacySql": False}},
            params=environment,
            dag=dag)
        if dependencies is not None and len(dependencies) > 0:
            for dependency in dependencies:
                dependency >> verify_task
        return verify_task

    def add_save_checkpoint_tasks(dependencies=None):
        def save_checkpoint(logical_date, **kwargs):
            with TemporaryDirectory() as tempdir:
                local_path = os.path.join(tempdir, "checkpoint.txt")
                remote_path = "checkpoint/block_date={block_date}/load_complete_checkpoint.txt".format(
                    block_date=logical_date.strftime("%Y-%m-%d")
                )
                open(local_path, mode='a').close()
                upload_to_gcs(
                    gcs_hook=GCSHook(gcp_conn_id="google_cloud_default"),
                    bucket=output_bucket,
                    object=remote_path,
                    filename=local_path)

        save_checkpoint_task = PythonOperator(
            task_id='save_checkpoint',
            python_callable=save_checkpoint,
            execution_timeout=timedelta(hours=1),
            dag=dag,
        )
        if dependencies is not None and len(dependencies) > 0:
            for dependency in dependencies:
                dependency >> save_checkpoint_task
        return save_checkpoint_task

    # Load tasks #

    load_blocks_task = add_load_tasks('blocks', 'json')
    load_transactions_task = add_load_tasks('transactions', 'json')
    load_receipts_task = add_load_tasks('receipts', 'json')
    load_logs_task = add_load_tasks('logs', 'json')
    load_contracts_task = add_load_tasks('contracts', 'json')
    load_tokens_task = add_load_tasks('tokens', 'json', allow_quoted_newlines=True)
    load_token_transfers_task = add_load_tasks('token_transfers', 'json')
    load_traces_task = add_load_tasks('traces', 'json')

    # Enrich tasks #

    enrich_blocks_task = add_enrich_tasks(
        'blocks', time_partitioning_field='timestamp', dependencies=[load_blocks_task])
    enrich_transactions_task = add_enrich_tasks(
        'transactions', dependencies=[load_blocks_task, load_transactions_task, load_receipts_task])
    enrich_logs_task = add_enrich_tasks(
        'logs', dependencies=[load_blocks_task, load_logs_task])
    enrich_token_transfers_task = add_enrich_tasks(
        'token_transfers', dependencies=[load_blocks_task, load_token_transfers_task])
    enrich_traces_task = add_enrich_tasks(
        'traces', dependencies=[load_blocks_task, load_traces_task])
    enrich_contracts_task = add_enrich_tasks(
        'contracts', dependencies=[load_blocks_task, load_contracts_task])
    enrich_tokens_task = add_enrich_tasks(
        'tokens', dependencies=[load_blocks_task, load_tokens_task])

    calculate_balances_task = add_enrich_tasks(
        'balances', dependencies=[enrich_blocks_task, enrich_transactions_task, enrich_traces_task],
        time_partitioning_field=None, always_load_all_partitions=True)

    # Verify tasks #

    verify_blocks_count_task = add_verify_tasks('blocks_count', [enrich_blocks_task])
    verify_blocks_have_latest_task = add_verify_tasks('blocks_have_latest', [enrich_blocks_task])
    verify_transactions_count_task = add_verify_tasks('transactions_count',
                                                      [enrich_blocks_task, enrich_transactions_task])
    verify_transactions_have_latest_task = add_verify_tasks('transactions_have_latest', [enrich_transactions_task])
    verify_logs_have_latest_task = add_verify_tasks('logs_have_latest', [enrich_logs_task])
    verify_logs_count_task = add_verify_tasks('logs_count', [enrich_logs_task])
    verify_token_transfers_have_latest_task = add_verify_tasks('token_transfers_have_latest',
                                                               [enrich_token_transfers_task])
    verify_traces_blocks_count_task = add_verify_tasks('traces_blocks_count', [enrich_blocks_task, enrich_traces_task])
    verify_traces_transactions_count_task = add_verify_tasks(
        'traces_transactions_count', [enrich_transactions_task, enrich_traces_task])
    verify_traces_contracts_count_task = add_verify_tasks(
        'traces_contracts_count', [enrich_transactions_task, enrich_traces_task, enrich_contracts_task])

    # Save checkpoint tasks #

    save_checkpoint_task = add_save_checkpoint_tasks(dependencies=[
        verify_blocks_count_task,
        verify_blocks_have_latest_task,
        verify_transactions_count_task,
        verify_transactions_have_latest_task,
        verify_logs_have_latest_task,
        verify_logs_count_task,
        verify_token_transfers_have_latest_task,
        verify_traces_blocks_count_task,
        verify_traces_transactions_count_task,
        verify_traces_contracts_count_task,
        enrich_tokens_task,
        calculate_balances_task,
    ])

    # Send email task #

    if notification_emails and len(notification_emails) > 0:
        send_email_task = EmailOperator(
            task_id='send_email',
            to=[email.strip() for email in notification_emails.split(',')],
            subject='Ethereum ETL Airflow Load DAG Succeeded',
            html_content='Ethereum ETL Airflow Load DAG Succeeded - {}'.format(chain),
            dag=dag
        )
        save_checkpoint_task >> send_email_task

    return dag


def adjust_schema_for_kovan(dag_id, task, schema):
    result = []
    if 'kovan' in dag_id and task == 'blocks':
        for field in schema:
            # nonce can be empty in Kovan
            if field.name == 'nonce':
                result.append(SchemaField(
                    name=field.name,
                    field_type=field.field_type,
                    mode="NULLABLE",
                    description=field.description,
                    fields=field.fields
                ))
            elif field.name == 'difficulty' or field.name == 'total_difficulty':
                result.append(SchemaField(
                    name=field.name,
                    field_type='FLOAT64',
                    mode=field.mode,
                    description=field.description,
                    fields=field.fields
                ))
            else:
                result.append(field)
    else:
        result = schema
    return result
