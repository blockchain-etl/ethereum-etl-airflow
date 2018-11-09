from __future__ import print_function

import os
from datetime import datetime, timedelta

from airflow import models
from airflow.operators import bash_operator


def get_boolean_env_variable(env_variable_name, default=True):
    raw_env = os.environ.get(env_variable_name)
    if raw_env is None or len(raw_env) == 0:
        return default
    else:
        return raw_env.lower() in ['true', 'yes']


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
    'ethereumetl_export_dag',
    # Daily at 1am
    schedule_interval='0 1 * * *',
    default_args=default_dag_args)
# miniconda.tar contains Python home directory with ethereum-etl dependencies install via pip
# Will get rid of this once Google Cloud Composer supports Python 3
install_python3_command = \
    'cp $DAGS_FOLDER/resources/miniconda.tar . && ' \
    'tar xvf miniconda.tar > untar_miniconda.log && ' \
    'PYTHON3=$PWD/miniconda/bin/python3'

setup_command = \
    'set -o xtrace && set -o pipefail && ' + install_python3_command + \
    ' && ' \
    'git clone --branch $ETHEREUMETL_REPO_BRANCH http://github.com/medvedev1088/ethereum-etl && cd ethereum-etl && ' \
    'export LC_ALL=C.UTF-8 && ' \
    'export LANG=C.UTF-8 && ' \
    'BLOCK_RANGE=$($PYTHON3 get_block_range_for_date.py -d $EXECUTION_DATE -p $WEB3_PROVIDER_URI) && ' \
    'BLOCK_RANGE_ARRAY=(${BLOCK_RANGE//,/ }) && START_BLOCK=${BLOCK_RANGE_ARRAY[0]} && END_BLOCK=${BLOCK_RANGE_ARRAY[1]} && ' \
    'EXPORT_LOCATION_URI=gs://$OUTPUT_BUCKET/export && ' \
    'export CLOUDSDK_PYTHON=/usr/local/bin/python'

export_blocks_and_transactions_command = \
    setup_command + ' && ' + \
    'echo $BLOCK_RANGE > blocks_meta.txt && ' \
    '$PYTHON3 export_blocks_and_transactions.py -b $EXPORT_BATCH_SIZE -w $EXPORT_MAX_WORKERS -s $START_BLOCK -e $END_BLOCK ' \
    '-p $WEB3_PROVIDER_URI --blocks-output blocks.csv --transactions-output transactions.csv && ' \
    'gsutil cp blocks.csv $EXPORT_LOCATION_URI/blocks/block_date=$EXECUTION_DATE/blocks.csv && ' \
    'gsutil cp transactions.csv $EXPORT_LOCATION_URI/transactions/block_date=$EXECUTION_DATE/transactions.csv && ' \
    'gsutil cp blocks_meta.txt $EXPORT_LOCATION_URI/blocks_meta/block_date=$EXECUTION_DATE/blocks_meta.txt '

export_receipts_and_logs_command = \
    setup_command + ' && ' + \
    'gsutil cp $EXPORT_LOCATION_URI/transactions/block_date=$EXECUTION_DATE/transactions.csv transactions.csv && ' \
    '$PYTHON3 extract_csv_column.py -i transactions.csv -o transaction_hashes.txt -c hash && ' \
    '$PYTHON3 export_receipts_and_logs.py -b $EXPORT_BATCH_SIZE -w $EXPORT_MAX_WORKERS --transaction-hashes transaction_hashes.txt ' \
    '-p $WEB3_PROVIDER_URI --receipts-output receipts.csv --logs-output logs.json && ' \
    'gsutil cp receipts.csv $EXPORT_LOCATION_URI/receipts/block_date=$EXECUTION_DATE/receipts.csv && ' \
    'gsutil cp logs.json $EXPORT_LOCATION_URI/logs/block_date=$EXECUTION_DATE/logs.json '

export_contracts_command = \
    setup_command + ' && ' + \
    'gsutil cp $EXPORT_LOCATION_URI/receipts/block_date=$EXECUTION_DATE/receipts.csv receipts.csv && ' \
    '$PYTHON3 extract_csv_column.py -i receipts.csv -o contract_addresses.txt -c contract_address && ' \
    '$PYTHON3 export_contracts.py -b $EXPORT_BATCH_SIZE -w $EXPORT_MAX_WORKERS --contract-addresses contract_addresses.txt ' \
    '-p $WEB3_PROVIDER_URI --output contracts.json && ' \
    'gsutil cp contracts.json $EXPORT_LOCATION_URI/contracts/block_date=$EXECUTION_DATE/contracts.json '

export_tokens_command = \
    setup_command + ' && ' + \
    'gsutil cp $EXPORT_LOCATION_URI/contracts/block_date=$EXECUTION_DATE/contracts.json contracts.json && ' \
    '$PYTHON3 filter_items.py -i contracts.json -p "item[\'is_erc20\'] or item[\'is_erc721\']" | ' \
    '$PYTHON3 extract_field.py -f address -o token_addresses.txt && ' \
    '$PYTHON3 export_tokens.py -b $EXPORT_BATCH_SIZE -w $EXPORT_MAX_WORKERS --token-addresses token_addresses.txt ' \
    '-p $WEB3_PROVIDER_URI --output tokens.csv && ' \
    'gsutil cp tokens.csv $EXPORT_LOCATION_URI/tokens/block_date=$EXECUTION_DATE/tokens.csv '

extract_token_transfers_command = \
    setup_command + ' && ' + \
    'gsutil cp $EXPORT_LOCATION_URI/logs/block_date=$EXECUTION_DATE/logs.json logs.json && ' \
    '$PYTHON3 extract_token_transfers.py -b $EXPORT_BATCH_SIZE -w $EXPORT_MAX_WORKERS --logs logs.json --output token_transfers.csv && ' \
    'gsutil cp token_transfers.csv $EXPORT_LOCATION_URI/token_transfers/block_date=$EXECUTION_DATE/token_transfers.csv '

# TODO: Test that command will fail if there are no blocks for the requested range.
export_traces_command = \
    setup_command + ' && ' + \
    'sleep $(( ( RANDOM % 300 ) + 1 )) && ' \
    '$PYTHON3 export_traces.py -b $EXPORT_BATCH_SIZE -w $EXPORT_MAX_WORKERS -s $START_BLOCK -e $END_BLOCK ' \
    '-p $WEB3_PROVIDER_URI_ARCHIVAL -o traces.csv && ' \
    'gsutil cp traces.csv $EXPORT_LOCATION_URI/traces/block_date=$EXECUTION_DATE/traces.csv '

output_bucket = os.environ.get('OUTPUT_BUCKET')
if output_bucket is None:
    raise ValueError('You must set OUTPUT_BUCKET environment variable')
web3_provider_uri = os.environ.get('WEB3_PROVIDER_URI', 'https://mainnet.infura.io/')
web3_provider_uri_archival = os.environ.get('WEB3_PROVIDER_URI_ARCHIVAL', web3_provider_uri)
ethereumetl_repo_branch = os.environ.get('ETHEREUMETL_REPO_BRANCH', 'master')
dags_folder = os.environ.get('DAGS_FOLDER', '/home/airflow/gcs/dags')
export_max_workers = os.environ.get('EXPORT_MAX_WORKERS', '5')
export_batch_size = os.environ.get('EXPORT_BATCH_SIZE', '10')

# ds is 1 day behind the date on which the run is scheduled, e.g. if the dag is scheduled to run at
# 1am on January 2, ds will be January 1.
environment = {
    'EXECUTION_DATE': '{{ ds }}',
    'ETHEREUMETL_REPO_BRANCH': ethereumetl_repo_branch,
    'WEB3_PROVIDER_URI': web3_provider_uri,
    'WEB3_PROVIDER_URI_ARCHIVAL': web3_provider_uri_archival,
    'OUTPUT_BUCKET': output_bucket,
    'DAGS_FOLDER': dags_folder,
    'EXPORT_MAX_WORKERS': export_max_workers,
    'EXPORT_BATCH_SIZE': export_batch_size
}


def add_export_task(toggle, task_id, bash_command, dependencies=None):
    if toggle:
        operator = bash_operator.BashOperator(
            task_id=task_id,
            bash_command=bash_command,
            execution_timeout=timedelta(hours=15),
            env=environment,
            dag=dag
        )
        if dependencies is not None and len(dependencies) > 0:
            for dependency in dependencies:
                if dependency is not None:
                    dependency >> operator
        return operator
    else:
        return None


export_blocks_and_transactions = get_boolean_env_variable('EXPORT_BLOCKS_AND_TRANSACTIONS', True)
export_receipts_and_logs = get_boolean_env_variable('EXPORT_RECEIPTS_AND_LOGS', True)
export_contracts = get_boolean_env_variable('EXPORT_CONTRACTS', True)
export_tokens = get_boolean_env_variable('EXPORT_TOKENS', True)
extract_token_transfers = get_boolean_env_variable('EXTRACT_TOKEN_TRANSFERS', True)
export_traces = get_boolean_env_variable('EXPORT_TRACES', True)

export_blocks_and_transactions_operator = add_export_task(
    export_blocks_and_transactions, 'export_blocks_and_transactions', export_blocks_and_transactions_command)

export_receipts_and_logs_operator = add_export_task(
    export_receipts_and_logs, 'export_receipts_and_logs', export_receipts_and_logs_command,
    dependencies=[export_blocks_and_transactions_operator])

export_contracts_operator = add_export_task(
    export_contracts, 'export_contracts', export_contracts_command,
    dependencies=[export_receipts_and_logs_operator])

export_tokens_operator = add_export_task(
    export_tokens, 'export_tokens', export_tokens_command,
    dependencies=[export_contracts_operator])

extract_token_transfers_operator = add_export_task(
    extract_token_transfers, 'extract_token_transfers', extract_token_transfers_command,
    dependencies=[export_receipts_and_logs_operator])

export_traces_operator = add_export_task(
    export_traces, 'export_traces', export_traces_command)
