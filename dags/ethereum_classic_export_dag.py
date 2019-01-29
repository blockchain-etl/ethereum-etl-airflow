from __future__ import print_function

from datetime import datetime

from airflow.models import Variable

from build_export_dag import build_export_dag, parse_bool

start_date = Variable.get('ethereum_classic_export_start_date', '2015-07-30')
options_args = {
    'export_daofork_traces_option': parse_bool(Variable.get('ethereum_classic_export_daofork_traces_option', 'False')),
    'export_genesis_traces_option': parse_bool(Variable.get('ethereum_classic_export_genesis_traces_option', 'True')),
    'export_blocks_and_transactions_toggle': parse_bool(Variable.get('ethereum_classic_export_blocks_and_transactions_toggle', 'True')),
    'export_receipts_and_logs_toggle': parse_bool(Variable.get('ethereum_classic_export_receipts_and_logs_toggle', 'True')),
    'export_contracts_toggle': parse_bool(Variable.get('ethereum_classic_export_contracts_toggle', 'True')),
    'export_tokens_toggle': parse_bool(Variable.get('ethereum_classic_export_tokens_toggle', 'True')),
    'extract_token_transfers_toggle': parse_bool(Variable.get('ethereum_classic_extract_token_transfers_toggle', 'True')),
    'export_traces_toggle': parse_bool(Variable.get('ethereum_classic_export_traces_toggle', 'True'))
}

provider_uri = Variable.get('ethereum_classic_provider_uri')
provider_uri_archival = Variable.get('ethereum_classic_provider_uri_archival', provider_uri)

DAG = build_export_dag(
    dag_id='ethereum_classic_export_dag',
    web3_provider_uri=provider_uri,
    web3_provider_uri_archival=provider_uri_archival,
    output_bucket=Variable.get('ethereum_classic_output_bucket'),
    start_date=datetime.strptime(start_date, '%Y-%m-%d'),
    notifications_emails=Variable.get('notification_emails', ''),
    schedule_interval='0 2 * * *',
    export_max_workers=10,
    export_batch_size=10,
    **options_args
)
