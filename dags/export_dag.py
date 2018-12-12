import os
from datetime import datetime, timedelta

from airflow import models
from airflow.operators import python_operator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

from ethereumetl.cli import (
    get_block_range_for_date,
    extract_csv_column,
    filter_items,
    extract_field,
    export_blocks_and_transactions,
    export_receipts_and_logs,
    export_contracts,
    export_tokens,
    extract_token_transfers,
    export_traces,
)

# DAG configuration

default_dag_args = {
    "depends_on_past": False,
    # TODO: restore start date
    "start_date": datetime(2016, 7, 30),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

notification_emails = os.environ.get("NOTIFICATION_EMAILS")
if notification_emails and len(notification_emails) > 0:
    default_dag_args["email"] = [email.strip() for email in notification_emails.split(",")]

# Define a DAG (directed acyclic graph) of tasks.
dag = models.DAG(
    "ethereumetl_export_dag",
    # Daily at 1am
    # TODO: change back to daily
    schedule_interval="0 1 1 1 *",
    default_args=default_dag_args,
)

# Environment configuration


def get_boolean_env_variable(env_variable_name, default=True):
    raw_env = os.environ.get(env_variable_name)
    if raw_env is None or len(raw_env) == 0:
        return default
    else:
        return raw_env.lower() in ["true", "yes"]


output_bucket = os.environ.get("OUTPUT_BUCKET")
if output_bucket is None:
    raise ValueError("You must set OUTPUT_BUCKET environment variable")

web3_provider_uri = os.environ.get("WEB3_PROVIDER_URI", "https://mainnet.infura.io/")
web3_provider_uri_archival = os.environ.get("WEB3_PROVIDER_URI_ARCHIVAL", web3_provider_uri)

export_max_workers = int(os.environ.get("EXPORT_MAX_WORKERS", 5))
export_batch_size = int(os.environ.get("EXPORT_BATCH_SIZE", 10))

export_daofork_traces_option = get_boolean_env_variable("EXPORT_DAOFORK_TRACES_OPTION")
export_genesis_traces_option = get_boolean_env_variable("EXPORT_GENESIS_TRACES_OPTION")

export_blocks_and_transactions_toggle = get_boolean_env_variable(
    "EXPORT_BLOCKS_AND_TRANSACTIONS", True
)
export_receipts_and_logs_toggle = get_boolean_env_variable("EXPORT_RECEIPTS_AND_LOGS", True)
export_contracts_toggle = get_boolean_env_variable("EXPORT_CONTRACTS", True)
export_tokens_toggle = get_boolean_env_variable("EXPORT_TOKENS", True)
extract_token_transfers_toggle = get_boolean_env_variable("EXTRACT_TOKEN_TRANSFERS", True)
export_traces_toggle = get_boolean_env_variable("EXPORT_TRACES", True)

# Export


def temporary_file(filename):
    return "/tmp/{filename}".format(filename=filename)


def export_path(folder, date):
    # TODO: use export folder
    return "export-dev/{folder}/block_date={block_date}/".format(
        folder=folder, block_date=date.strftime("%Y-%m-%d")
    )


cloud_storage_hook = GoogleCloudStorageHook(google_cloud_storage_conn_id="google_cloud_default")


def temporary_to_export_location(filename, export_path):
    cloud_storage_hook.upload(
        bucket=output_bucket, object=export_path + filename, filename=temporary_file(filename)
    )


def temporary_from_export_location(export_path, filename):
    cloud_storage_hook.download(
        bucket=output_bucket, object=export_path + filename, filename=temporary_file(filename)
    )


def get_block_range(date):
    get_block_range_for_date.callback(
        provider_uri=web3_provider_uri, date=date, output=temporary_file("blocks_meta.txt")
    )

    with open(temporary_file("blocks_meta.txt")) as block_range_file:
        block_range = block_range_file.read()
        start_block, end_block = block_range.split(",")

    return int(start_block), int(end_block)


def export_blocks_and_transactions_command(execution_date, **kwargs):
    start_block, end_block = get_block_range(execution_date)

    export_blocks_and_transactions.callback(
        start_block=start_block,
        end_block=end_block,
        batch_size=export_batch_size,
        provider_uri=web3_provider_uri,
        max_workers=export_max_workers,
        blocks_output=temporary_file("blocks.csv"),
        transactions_output=temporary_file("transactions.csv"),
    )

    temporary_to_export_location("blocks_meta.txt", export_path("blocks_meta", execution_date))
    temporary_to_export_location("blocks.csv", export_path("blocks", execution_date))
    temporary_to_export_location("transactions.csv", export_path("transactions", execution_date))


def export_receipts_and_logs_command(execution_date, **kwargs):
    temporary_from_export_location(export_path("transactions", execution_date), "transactions.csv")

    extract_csv_column.callback(
        input=temporary_file("transactions.csv"),
        output=temporary_file("transaction_hashes.txt"),
        column="hash",
    )

    export_receipts_and_logs.callback(
        batch_size=export_batch_size,
        transaction_hashes=temporary_file("transaction_hashes.txt"),
        provider_uri=web3_provider_uri,
        max_workers=export_max_workers,
        receipts_output=temporary_file("receipts.csv"),
        logs_output=temporary_file("logs.json"),
    )

    temporary_to_export_location("receipts.csv", export_path("receipts", execution_date))
    temporary_to_export_location("logs.json", export_path("logs", execution_date))


def export_contracts_command(execution_date, **kwargs):
    temporary_from_export_location(export_path("receipts", execution_date), "receipts.csv")

    extract_csv_column.callback(
        input=temporary_file("receipts.csv"),
        output=temporary_file("contract_addresses.txt"),
        column="contract_address",
    )

    export_contracts.callback(
        batch_size=export_batch_size,
        contract_addresses=temporary_file("contract_addresses.txt"),
        output=temporary_file("contracts.json"),
        max_workers=export_max_workers,
        provider_uri=web3_provider_uri,
    )

    temporary_to_export_location("contracts.json", export_path("contracts", execution_date))


def export_tokens_command(execution_date, **kwargs):
    temporary_from_export_location(export_path("contracts", execution_date), "contracts.json")

    filter_items.callback(
        input=temporary_file("contracts.json"),
        output=temporary_file("token_contracts.json"),
        predicate="item['is_erc20'] or item['is_erc721']",
    )

    extract_field.callback(
        input=temporary_file("token_contracts.json"),
        output=temporary_file("token_addresses.txt"),
        field="address",
    )

    export_tokens.callback(
        token_addresses=temporary_file("token_addresses.txt"),
        output=temporary_file("tokens.csv"),
        max_workers=export_max_workers,
        provider_uri=web3_provider_uri,
    )

    temporary_to_export_location("tokens.csv", export_path("tokens", execution_date))


def extract_token_transfers_command(execution_date, **kwargs):
    temporary_from_export_location(export_path("logs", execution_date), "logs.json")

    extract_token_transfers.callback(
        logs=temporary_file("logs.json"),
        batch_size=export_batch_size,
        output=temporary_file("token_transfers.csv"),
        max_workers=export_max_workers,
    )

    temporary_to_export_location(
        "token_transfers.csv", export_path("token_transfers", execution_date)
    )


def export_traces_command(execution_date, **kwargs):
    start_block, end_block = get_block_range(execution_date)

    export_traces.callback(
        start_block=start_block,
        end_block=end_block,
        batch_size=export_batch_size,
        output=temporary_file("traces.csv"),
        max_workers=export_max_workers,
        provider_uri=web3_provider_uri_archival,
        genesis_traces=export_genesis_traces_option,
        daofork_traces=export_daofork_traces_option,
    )

    temporary_to_export_location("traces.csv", export_path("traces", execution_date))


def add_export_task(toggle, task_id, python_callable, dependencies=None):
    if toggle:
        operator = python_operator.PythonOperator(
            task_id=task_id,
            python_callable=python_callable,
            provide_context=True,
            execution_timeout=timedelta(hours=15),
            dag=dag,
        )
        if dependencies is not None and len(dependencies) > 0:
            for dependency in dependencies:
                if dependency is not None:
                    dependency >> operator
        return operator
    else:
        return None


# Operators

export_blocks_and_transactions_operator = add_export_task(
    export_blocks_and_transactions_toggle,
    "export_blocks_and_transactions",
    export_blocks_and_transactions_command,
)

export_receipts_and_logs_operator = add_export_task(
    export_receipts_and_logs_toggle,
    "export_receipts_and_logs",
    export_receipts_and_logs_command,
    dependencies=[export_blocks_and_transactions_operator],
)

export_contracts_operator = add_export_task(
    export_contracts_toggle,
    "export_contracts",
    export_contracts_command,
    dependencies=[export_receipts_and_logs_operator],
)

export_tokens_operator = add_export_task(
    export_tokens_toggle,
    "export_tokens",
    export_tokens_command,
    dependencies=[export_contracts_operator],
)

extract_token_transfers_operator = add_export_task(
    extract_token_transfers_toggle,
    "extract_token_transfers",
    extract_token_transfers_command,
    dependencies=[export_receipts_and_logs_operator],
)

export_traces_operator = add_export_task(
    export_traces_toggle, "export_traces", export_traces_command
)
