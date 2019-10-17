from __future__ import print_function

import os

from airflow.models import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from dags.ethereumetl_airflow.utils import _build_clickhouse_http_command, _build_setup_table_operator


CLICKHOUSE_URI = Variable.get('clickhouse_uri', '')
ENRICH_QUERY_CHAIN = (
    'staging/flush',
    'staging/enrich',
    'staging/insert',
    'staging/flush',
    'staging/drop'
)

def build_load_clickhouse_dag(
        dag_id,
        output_bucket,
        load_start_date,
        chain='ethereum',
        notification_emails=None,
        load_schedule_interval='0 0 * * *',
        load_max_workers=5,
        load_batch_size=10,
        load_max_active_runs=10
):
    load_max_workers = str(load_max_workers)
    load_batch_size = str(load_batch_size)
    dags_folder = os.environ.get('DAGS_FOLDER', '/home/airflow/gcs/dags')

    default_dag_args = {
        'depends_on_past': True,
        'start_date': load_start_date,
        'email_on_failure': True,
        'email_on_retry': True,
        'retries': 10,
        'retry_delay': timedelta(minutes=5),
    }

    env = {
        'CHAIN': chain,
        'EXECUTION_DATE': '{{ ds }}',
        'EXECUTION_DATE_NODASH': '{{ ds_nodash }}',
        'GCS_BUCKET': output_bucket,
        'DAG_FOLDER': dags_folder,
        'LOAD_MAX_WORKERS': load_max_workers,
        'LOAD_BATCH_SIZE': load_batch_size,
        'CLICKHOUSE_URI': Variable.get('clickhouse_uri', '')
    }

    if notification_emails and len(notification_emails) > 0:
        default_dag_args['email'] = [email.strip() for email in notification_emails.split(',')]

    dag = DAG(
        dag_id,
        schedule_interval=load_schedule_interval,
        max_active_runs=load_max_active_runs,
        concurrency=10,
        default_args=default_dag_args,
    )

    SETUP_COMMAND = \
        'set -o xtrace && set -o pipefail && ' + \
        'export LC_ALL=C.UTF-8 && ' \
        'export LANG=C.UTF-8 && ' \
        'export CLOUDSDK_PYTHON=/usr/bin/python2'

    def _build_load_command(resource):
        parent_dir = os.path.join(dags_folder, 'resources/ethereumetl/load')
        file_path = f'"$CHAIN"_{resource}.json'

        CP_COMMAND = f'gsutil cp -Z gs://$GCS_BUCKET/export/{resource}/block_date=$EXECUTION_DATE.json "$CHAIN"_{resource}.json'

        LOAD_COMMAND = _build_clickhouse_http_command(
            parent_dir=parent_dir,
            resource=resource,
            filename=file_path
        )

        command = ' && '.join(
            [SETUP_COMMAND, CP_COMMAND, LOAD_COMMAND]
        )
        return command

    def _build_enrich_command(resource, chain):
        command = ' && '.join(
            [_build_clickhouse_http_command(parent_dir=link, resource=resource) for link in chain]
        )
        return command

    def add_clickhouse_operator(dag, env, task_id, bash_command, dependencies=None):
        operator = BashOperator(
            task_id=task_id,
            bash_command=bash_command,
            execution_timeout=timedelta(hours=15),
            env=env,
            dag=dag
        )
        if dependencies is not None and len(dependencies) > 0:
            for dependency in dependencies:
                if dependency is not None:
                    dependency >> operator
        return operator

    setup_blocks_operator = _build_setup_table_operator(dag=dag, env=env, table_type='staging', resource="blocks")


    load_blocks_operator = add_clickhouse_operator(
        dag=dag,
        env=env,
        task_id='load_blocks_operator',
        bash_command=_build_load_command(resource="blocks"),
        dependencies=[setup_blocks_operator]
    )

    add_clickhouse_operator(
        dag=dag,
        env=env,
        task_id='enrich_blocks_operator',
        bash_command=_build_enrich_command(resource="blocks", chain=ENRICH_QUERY_CHAIN),
        dependencies=[load_blocks_operator]
    )

    return dag