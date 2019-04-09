from datetime import datetime

from airflow.models import Variable


def read_export_dag_vars(var_prefix, **kwargs):
    export_start_date = read_var('export_start_date', var_prefix, True, **kwargs)
    export_start_date = datetime.strptime(export_start_date, '%Y-%m-%d')
    
    provider_uri = read_var('provider_uri', var_prefix, True, **kwargs)
    backup_provider_uri_list = read_var('backup_provider_uri_list', var_prefix, True, **kwargs)
    provider_uri_archival = read_var('provider_uri_archival', var_prefix, False, **kwargs)
    if provider_uri_archival is None:
        provider_uri_archival = provider_uri
    cloud_provider = read_var('cloud_provider', var_prefix, False, **kwargs)
    if cloud_provider is None:
        cloud_provider = 'gcp'

    vars = {
        'output_bucket': read_var('output_bucket', var_prefix, True, **kwargs),
        'cloud_provider': cloud_provider,
        'export_start_date': export_start_date,
        'export_schedule_interval': read_var('export_schedule_interval', var_prefix, True, **kwargs),
        'provider_uri': provider_uri,
        'backup_provider_uri_list':backup_provider_uri_list,
        'provider_uri_archival': provider_uri_archival,
        'notification_emails': read_var('notification_emails', None, False, **kwargs),
        'export_max_active_runs': read_var('export_max_active_runs', var_prefix, False, **kwargs),
        'export_max_workers': int(read_var('export_max_workers', var_prefix, True, **kwargs)),
        'export_batch_size': int(read_var('export_batch_size', var_prefix, True, **kwargs)),
        # toggles
        'export_daofork_traces_option': parse_bool(
            read_var('export_daofork_traces_option', var_prefix, False, **kwargs)),
        'export_genesis_traces_option': parse_bool(
            read_var('export_genesis_traces_option', var_prefix, False, **kwargs)),
        'export_blocks_and_transactions_toggle': parse_bool(
            read_var('export_blocks_and_transactions_toggle', var_prefix, False, **kwargs)),
        'export_receipts_and_logs_toggle': parse_bool(
            read_var('export_receipts_and_logs_toggle', var_prefix, False, **kwargs)),
        'export_contracts_toggle': parse_bool(
            read_var('export_contracts_toggle', var_prefix, False, **kwargs)),
        'export_tokens_toggle': parse_bool(
            read_var('export_tokens_toggle', var_prefix, False, **kwargs)),
        'extract_token_transfers_toggle': parse_bool(
            read_var('extract_token_transfers_toggle', var_prefix, False, **kwargs)),
        'export_traces_toggle': parse_bool(
            read_var('export_traces_toggle', var_prefix, False, **kwargs)),
    }

    return vars


def read_load_dag_vars(var_prefix, **kwargs):
    vars = {
        'output_bucket': read_var('output_bucket', var_prefix, True, **kwargs),
        'destination_dataset_project_id': read_var('destination_dataset_project_id', var_prefix, True, **kwargs),
        'notification_emails': read_var('notification_emails', None, False, **kwargs),
        'schedule_interval': read_var('schedule_interval', var_prefix, True, **kwargs),
        'copy_dataset_project_id': read_var('copy_dataset_project_id', var_prefix, False, **kwargs),
        'copy_dataset_name': read_var('copy_dataset_name', var_prefix, False, **kwargs),
    }

    load_start_date = read_var('load_start_date', vars, False, **kwargs)
    if load_start_date is not None:
        load_start_date = datetime.strptime(load_start_date, '%Y-%m-%d')
        vars['load_start_date'] = load_start_date

    return vars


def read_load_dag_redshift_vars(var_prefix, **kwargs):
    vars = {
        'output_bucket': read_var('output_bucket', var_prefix, True, **kwargs),
        'aws_access_key_id':  read_var('aws_access_key_id', var_prefix, True, **kwargs),
        'aws_secret_access_key': read_var('aws_secret_access_key', var_prefix, True, **kwargs),
        'notification_emails': read_var('notification_emails', None, False, **kwargs),
        'schedule_interval': read_var('schedule_interval', var_prefix, True, **kwargs),
    }

    load_start_date = read_var('load_start_date', vars, False, **kwargs)
    if load_start_date is not None:
        load_start_date = datetime.strptime(load_start_date, '%Y-%m-%d')
        vars['load_start_date'] = load_start_date

    return vars


def read_var(var_name, var_prefix=None, required=False, **kwargs):
    full_var_name = f'{var_prefix}{var_name}' if var_prefix is not None else var_name
    var = Variable.get(full_var_name, '')
    var = var if var != '' else None
    if var is None:
        var = kwargs.get(var_name)
    if required and var is None:
        raise ValueError(f'{full_var_name} variable is required')
    return var


def parse_bool(bool_string, default=True):
    if isinstance(bool_string, bool):
        return bool_string
    if bool_string is None or len(bool_string) == 0:
        return default
    else:
        return bool_string.lower() in ["true", "yes"]
