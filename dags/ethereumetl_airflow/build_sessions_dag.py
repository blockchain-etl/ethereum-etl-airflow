from __future__ import print_function

import logging
import os

from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import ExternalTaskSensor
from datetime import datetime, timedelta
from google.cloud import bigquery

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)


def build_sessions_dag(
        dag_id,
        load_dag_id,
        sql_dir,
        source_project_id,
        source_dataset_name,
        destination_project_id,
        destination_dataset_name,
        notification_emails=None,
        schedule_interval='0 14 * * *',
        start_date=datetime(2015, 7, 30),
        environment='prod'
):

    default_dag_args = {
        'depends_on_past': False,
        'start_date': start_date,
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
        catchup=True,
        schedule_interval=schedule_interval,
        default_args=default_dag_args)

    def read_file(filepath):
        with open(filepath) as file_handle:
            content = file_handle.read()
            return content

    def add_sessions_task(task, dependencies=None):
        def sessions_task(ds, **kwargs):

            client = bigquery.Client()
            sql_path = os.path.join(sql_dir, '{task}.sql'.format(task=task))
            sql_template = read_file(sql_path)
            ds_no_dashes = ds.replace('-', '')
            sql = sql_template.format(
                ds=ds,
                ds_no_dashes=ds_no_dashes,
                source_project_id=source_project_id,
                source_dataset_name=source_dataset_name,
                destination_project_id=destination_project_id,
                destination_dataset_name=destination_dataset_name,
            )
            print(sql)
            query_job = client.query(sql)
            result = query_job.result()
            logging.info(result)

        sessions_operator = PythonOperator(
            task_id=f'{task}',
            # Necessary because we use traces overlapping the previous execution date.
            depends_on_past=True,
            wait_for_downstream=True,
            python_callable=sessions_task,
            provide_context=True,
            execution_timeout=timedelta(minutes=60),
            dag=dag
        )

        if dependencies is not None and len(dependencies) > 0:
            for dependency in dependencies:
                dependency >> sessions_operator
        return sessions_operator

    stage_root_call_traces_task = add_sessions_task('root_call_traces')
    upsert_sessions_task = add_sessions_task('sessions')

    # Dummy task indicating successful DAG completion.
    done_task = BashOperator(
        task_id='done',
        bash_command='echo done',
        dag=dag
    )

    #
    # Task sensor is enabled only in production for now because our load DAG is
    # not running in the lower environments.
    #
    if environment == 'prod':
        wait_for_ethereum_load_dag_task = ExternalTaskSensor(
            task_id='wait_for_ethereum_load_dag',
            external_dag_id=load_dag_id,
            external_task_id='send_email', # Task sends a notification on DAG completion.
            execution_delta=timedelta(hours=1),
            priority_weight=0,
            mode='reschedule',
            poke_interval=5 * 60,
            timeout=60 * 60 * 12,
            dag=dag
        )
        wait_for_ethereum_load_dag_task >> stage_root_call_traces_task

    stage_root_call_traces_task >> upsert_sessions_task
    upsert_sessions_task >> done_task

    return dag
