from __future__ import print_function

from datetime import datetime

from airflow.models import Variable

from build_export_dag import build_export_dag

start_date = Variable.get('ethereum_export_start_date', '2015-07-30')
options_args = {
    'export_daofork_traces_option': Variable.get('ethereum_export_daofork_traces_option', True),
    'export_genesis_traces_option': Variable.get('ethereum_export_genesis_traces_option', True),
    'export_blocks_and_transactions_toggle': Variable.get('ethereum_export_blocks_and_transactions_toggle', True),
    'export_receipts_and_logs_toggle': Variable.get('ethereum_export_receipts_and_logs_toggle', True),
    'export_contracts_toggle': Variable.get('ethereum_export_contracts_toggle', True),
    'export_tokens_toggle': Variable.get('ethereum_export_tokens_toggle', True),
    'extract_token_transfers_toggle': Variable.get('ethereum_extract_token_transfers_toggle', True),
    'export_traces_toggle': Variable.get('ethereum_export_traces_toggle', True)
}

DAG = build_export_dag(
    dag_id='ethereum_export_dag',
    web3_provider_uri=Variable.get('ethereum_provider_uri'),
    web3_provider_uri_archival=Variable.get('ethereum_provider_uri_archival'),
    output_bucket=Variable.get('ethereum_output_bucket'),
    cloud_provider=Variable.get('cloud_provider', 'gcp'),
    start_date=datetime.strptime(start_date, '%Y-%m-%d'),
    chain='ethereum',
    notifications_emails=Variable.get('notification_emails', ''),
    schedule_interval='0 8 * * *',
    export_max_workers=10,
    export_batch_size=10,
    **options_args
)
