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
        'retries': 20,
        'retry_delay': timedelta(minutes=1),
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
        concurrency=5,
        default_args=default_dag_args
    )

    SETUP_COMMAND = \
        'set -o xtrace && ' + \
        'export LC_ALL=C.UTF-8 && ' \
        'export LANG=C.UTF-8 && ' \
        'export CLOUDSDK_PYTHON=/usr/bin/python2'

    def _build_load_command(resource):
        parent_dir = os.path.join(dags_folder, 'resources/ethereumetl/staging/load')
        file_path = f'"$CHAIN"_{resource}'
        resource_name = resource.split('.')[0]

        CP_COMMAND = f'gsutil cp -Z gs://$GCS_BUCKET/export/{resource_name}/block_date=$EXECUTION_DATE/{resource} "$CHAIN"_{resource}'

        LOAD_COMMAND = _build_clickhouse_http_command(
            parent_dir=parent_dir,
            resource=resource_name,
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
    setup_contracts_operator = _build_setup_table_operator(dag=dag, env=env, table_type='staging', resource="contracts")
    setup_logs_operator = _build_setup_table_operator(dag=dag, env=env, table_type='staging', resource="logs")
    setup_receipts_operator = _build_setup_table_operator(dag=dag, env=env, table_type='staging', resource="receipts")
    setup_tokens_operator = _build_setup_table_operator(dag=dag, env=env, table_type='staging', resource="tokens")
    setup_token_transfers_operator = _build_setup_table_operator(dag=dag, env=env, table_type='staging', resource="token_transfers")
    setup_traces_operator = _build_setup_table_operator(dag=dag, env=env, table_type='staging', resource="traces")
    setup_transactions_operator = _build_setup_table_operator(dag=dag, env=env, table_type='staging', resource="transactions")


    load_blocks_operator = add_clickhouse_operator(
        dag=dag,
        env=env,
        task_id='load_blocks_operator',
        bash_command=_build_load_command(resource="blocks.csv"),
        dependencies=[setup_blocks_operator]
    )

    load_contracts_operator = add_clickhouse_operator(
        dag=dag,
        env=env,
        task_id='load_contracts_operator',
        bash_command=_build_load_command(resource="contracts.json"),
        dependencies=[setup_contracts_operator]
    )

    load_logs_operator = add_clickhouse_operator(
        dag=dag,
        env=env,
        task_id='load_logs_operator',
        bash_command=_build_load_command(resource="logs.json"),
        dependencies=[setup_logs_operator]
    )

    load_receipts_operator = add_clickhouse_operator(
        dag=dag,
        env=env,
        task_id='load_receipts_operator',
        bash_command=_build_load_command(resource="receipts.csv"),
        dependencies=[setup_receipts_operator]
    )


    load_tokens_operator = add_clickhouse_operator(
        dag=dag,
        env=env,
        task_id='load_tokens_operator',
        bash_command=_build_load_command(resource="tokens.csv"),
        dependencies=[setup_tokens_operator]
    )


    load_token_transfers_operator = add_clickhouse_operator(
        dag=dag,
        env=env,
        task_id='load_token_transfers_operator',
        bash_command=_build_load_command(resource="token_transfers.csv"),
        dependencies=[setup_token_transfers_operator]
    )


    load_traces_operator = add_clickhouse_operator(
        dag=dag,
        env=env,
        task_id='load_traces_operator',
        bash_command=_build_load_command(resource="traces.csv"),
        dependencies=[setup_traces_operator]
    )


    load_transactions_operator = add_clickhouse_operator(
        dag=dag,
        env=env,
        task_id='load_transactions_operator',
        bash_command=_build_load_command(resource="transactions.csv"),
        dependencies=[setup_transactions_operator]
    )

    enrich_blocks_operator = add_clickhouse_operator(
        dag=dag,
        env=env,
        task_id='enrich_blocks_operator',
        bash_command=_build_enrich_command(resource="blocks", chain=ENRICH_QUERY_CHAIN),
        dependencies=[load_blocks_operator]
    )

    add_clickhouse_operator(
        dag=dag,
        env=env,
        task_id='enrich_contracts_operator',
        bash_command=_build_enrich_command(resource="contracts", chain=ENRICH_QUERY_CHAIN),
        dependencies=[load_contracts_operator, enrich_blocks_operator]
    )

    add_clickhouse_operator(
        dag=dag,
        env=env,
        task_id='enrich_logs_operator',
        bash_command=_build_enrich_command(resource="logs", chain=ENRICH_QUERY_CHAIN),
        dependencies=[load_logs_operator, enrich_blocks_operator]
    )

    enrich_receipts_operator = add_clickhouse_operator(
        dag=dag,
        env=env,
        task_id='enrich_receipts_operator',
        bash_command=_build_enrich_command(resource="receipts", chain=ENRICH_QUERY_CHAIN),
        dependencies=[load_receipts_operator, enrich_blocks_operator]
    )

    add_clickhouse_operator(
        dag=dag,
        env=env,
        task_id='enrich_tokens_operator',
        bash_command=_build_enrich_command(resource="tokens", chain=ENRICH_QUERY_CHAIN),
        dependencies=[load_tokens_operator, enrich_blocks_operator]
    )

    add_clickhouse_operator(
        dag=dag,
        env=env,
        task_id='enrich_token_transfers_operator',
        bash_command=_build_enrich_command(resource="token_transfers", chain=ENRICH_QUERY_CHAIN),
        dependencies=[load_token_transfers_operator, enrich_blocks_operator]
    )

    add_clickhouse_operator(
        dag=dag,
        env=env,
        task_id='enrich_traces_operator',
        bash_command=_build_enrich_command(resource="traces", chain=ENRICH_QUERY_CHAIN),
        dependencies=[load_traces_operator, enrich_blocks_operator]
    )

    add_clickhouse_operator(
        dag=dag,
        env=env,
        task_id='enrich_transactions_operator',
        bash_command=_build_enrich_command(resource="transactions", chain=ENRICH_QUERY_CHAIN),
        dependencies=[load_transactions_operator, enrich_blocks_operator, enrich_receipts_operator]
    )



    return dag