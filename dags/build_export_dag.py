from __future__ import print_function

import os
import logging
from datetime import timedelta
from tempfile import TemporaryDirectory

from airflow import DAG, configuration
from airflow.operators import python_operator

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

from web3 import Web3
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.service.eth_service import BlockTimestampGraph


def build_export_dag(
    dag_id,
    provider_uri,
    backup_provider_uri_list,
    provider_uri_archival,
    output_bucket,
    cloud_provider,
    export_start_date,
    notification_emails=None,
    export_schedule_interval='0 0 * * *',
    export_max_workers=10,
    export_batch_size=10,
    export_max_active_runs=None,
    **kwargs
):
    default_dag_args = {
        "depends_on_past": False,
        "start_date": export_start_date,
        "email_on_failure": True,
        "email_on_retry": True,
        "retries": 5,
        "retry_delay": timedelta(minutes=5)
    }

    if notification_emails and len(notification_emails) > 0:
        default_dag_args['email'] = [email.strip() for email in notification_emails.split(',')]

    export_daofork_traces_option = kwargs.get('export_daofork_traces_option')
    export_genesis_traces_option = kwargs.get('export_genesis_traces_option')
    export_blocks_and_transactions_toggle = kwargs.get('export_blocks_and_transactions_toggle')
    export_receipts_and_logs_toggle = kwargs.get('export_receipts_and_logs_toggle')
    export_contracts_toggle = kwargs.get('export_contracts_toggle')
    export_tokens_toggle = kwargs.get('export_tokens_toggle')
    extract_token_transfers_toggle = kwargs.get('extract_token_transfers_toggle')
    export_traces_toggle = kwargs.get('export_traces_toggle')

    if export_max_active_runs is None:
        export_max_active_runs = configuration.conf.getint('core', 'max_active_runs_per_dag')

    dag = DAG(
        dag_id,
        schedule_interval=export_schedule_interval,
        default_args=default_dag_args,
        max_active_runs=export_max_active_runs
    )

    if cloud_provider == 'aws':
        from airflow.hooks.S3_hook import S3Hook
        cloud_storage_hook = S3Hook(aws_conn_id="aws_default")
    else:
        from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
        cloud_storage_hook = GoogleCloudStorageHook(google_cloud_storage_conn_id="google_cloud_default")

    # Export
    def export_path(directory, date):
        return "export/{directory}/block_date={block_date}/".format(
            directory=directory, block_date=date.strftime("%Y-%m-%d")
        )

    def copy_to_export_path(file_path, export_path):
        logging.info('Calling copy_to_export_path({}, {})'.format(file_path, export_path))
        filename = os.path.basename(file_path)

        
        if cloud_provider == 'aws':
            cloud_storage_hook.load_file(
                filename=file_path,
                bucket_name=output_bucket,
                key=export_path + filename,
                replace=True,
                encrypt=False
            )
        else:
            upload_to_gcs(
                gcs_hook=cloud_storage_hook,
                bucket=output_bucket,
                object=export_path + filename,
                filename=file_path)

    def copy_from_export_path(export_path, file_path):
        logging.info('Calling copy_from_export_path({}, {})'.format(export_path, file_path))
        filename = os.path.basename(file_path)
        
        if cloud_provider == 'aws':
            # boto3.s3.Object
            s3_object = cloud_storage_hook.get_key(
                bucket_name=output_bucket,
                key=export_path + filename
            )
            s3_object.download_file(file_path)
        else:
            download_from_gcs(bucket=output_bucket, object=export_path + filename, filename=file_path)


    def node_watch_command(**kwargs):
        """
        This functions checks if the provider_uri is connected or not.
        input: kwargs
        returns: xcom_push either provider_uri or one of the uris from backup_provider_uri_list 
        to the dependencies. 
        Xcoms table in database needs to be cleaned up time by time
        """
        def connection_check_recursice(uri,backup_uri_list):
            """
            This function recursively check the connection of the backup uris.
            input: backup_uri_list is backup_provider_uri_list variable. needs to be comma
                seperated uris.
            returns: a live uri
            """
            if not backup_uri_list: 
                raise ConnectionError("No ethereum node is responding") 
            backup_provider_uri = backup_uri_list.pop()
            provider = get_provider_from_uri(uri)
            web3 = Web3(provider)
            block_timesstamp_graph = BlockTimestampGraph(web3)
            try:
                block_timesstamp_graph.get_first_point()
                return uri
            except:
                return connection_check_recursice(backup_provider_uri,backup_uri_list)
                
        task_instance = kwargs['ti']
        backup_uri_list = [uri.strip() for uri in backup_provider_uri_list.split(',')]
        uri = connection_check_recursice(provider_uri,backup_uri_list)
        
        return task_instance.xcom_push(key='live_uri',value=uri)



    def get_block_range(tempdir, date,provider_uri=provider_uri):
        logging.info('Calling get_block_range_for_date({}, {}, ...)'.format(provider_uri, date))
        get_block_range_for_date.callback(
            provider_uri=provider_uri, date=date, output=os.path.join(tempdir, "blocks_meta.txt")
        )

        with open(os.path.join(tempdir, "blocks_meta.txt")) as block_range_file:
            block_range = block_range_file.read()
            start_block, end_block = block_range.split(",")

        return int(start_block), int(end_block)

    def export_blocks_and_transactions_command(execution_date, **kwargs):
        task_instance = kwargs['ti']
        live_uri = task_instance.xcom_pull(key='live_uri',task_ids='node_watch')
        with TemporaryDirectory() as tempdir:
            start_block, end_block = get_block_range(tempdir, execution_date,provider_uri=live_uri)

            logging.info('Calling export_blocks_and_transactions({}, {}, {}, {}, {}, ...)'.format(
                start_block, end_block, export_batch_size, live_uri, export_max_workers))

            export_blocks_and_transactions.callback(
                start_block=start_block,
                end_block=end_block,
                batch_size=export_batch_size,
                provider_uri=live_uri,
                max_workers=export_max_workers,
                blocks_output=os.path.join(tempdir, "blocks.csv"),
                transactions_output=os.path.join(tempdir, "transactions.csv"),
            )

            copy_to_export_path(
                os.path.join(tempdir, "blocks_meta.txt"), export_path("blocks_meta", execution_date)
            )

            copy_to_export_path(
                os.path.join(tempdir, "blocks.csv"), export_path("blocks", execution_date)
            )

            copy_to_export_path(
                os.path.join(tempdir, "transactions.csv"), export_path("transactions", execution_date)
            )

    def export_receipts_and_logs_command(execution_date, **kwargs):
        task_instance = kwargs['ti']
        live_uri = task_instance.xcom_pull(key='live_uri',task_ids='node_watch')
        with TemporaryDirectory() as tempdir:
            copy_from_export_path(
                export_path("transactions", execution_date), os.path.join(tempdir, "transactions.csv")
            )

            logging.info('Calling extract_csv_column(...)')
            extract_csv_column.callback(
                input=os.path.join(tempdir, "transactions.csv"),
                output=os.path.join(tempdir, "transaction_hashes.txt"),
                column="hash",
            )

            logging.info('Calling export_receipts_and_logs({}, ..., {}, {}, ...)'.format(
                export_batch_size, live_uri, export_max_workers))
            export_receipts_and_logs.callback(
                batch_size=export_batch_size,
                transaction_hashes=os.path.join(tempdir, "transaction_hashes.txt"),
                provider_uri=live_uri,
                max_workers=export_max_workers,
                receipts_output=os.path.join(tempdir, "receipts.csv"),
                logs_output=os.path.join(tempdir, "logs.json"),
            )

            copy_to_export_path(
                os.path.join(tempdir, "receipts.csv"), export_path("receipts", execution_date)
            )
            copy_to_export_path(os.path.join(tempdir, "logs.json"), export_path("logs", execution_date))

    def export_contracts_command(execution_date, **kwargs):
        task_instance = kwargs['ti']
        live_uri = task_instance.xcom_pull(key='live_uri',task_ids='node_watch')
        with TemporaryDirectory() as tempdir:
            copy_from_export_path(
                export_path("traces", execution_date), os.path.join(tempdir, "traces.csv")
            )

            logging.info('Calling filter_items(...)')
            filter_items.callback(
                input=os.path.join(tempdir, "traces.csv"),
                output=os.path.join(tempdir, "traces_type_create.csv"),
                predicate="item['trace_type']=='create' and item['to_address'] is not None and len(item['to_address']) > 0",
            )

            logging.info('Removing unneeded file traces.csv')
            os.remove(os.path.join(tempdir, "traces.csv"))

            logging.info('Calling extract_field(...)')
            extract_field.callback(
                input=os.path.join(tempdir, "traces_type_create.csv"),
                output=os.path.join(tempdir, "contract_addresses.txt"),
                field="to_address",
            )

            logging.info('Removing unneeded file traces_type_create.csv')
            os.remove(os.path.join(tempdir, "traces_type_create.csv"))

            logging.info('Calling export_contracts({}, ..., {}, {})'.format(
                export_batch_size, export_max_workers, live_uri
            ))
            export_contracts.callback(
                batch_size=export_batch_size,
                contract_addresses=os.path.join(tempdir, "contract_addresses.txt"),
                output=os.path.join(tempdir, "contracts.json"),
                max_workers=export_max_workers,
                provider_uri=live_uri,
            )

            copy_to_export_path(
                os.path.join(tempdir, "contracts.json"), export_path("contracts", execution_date)
            )

    def export_tokens_command(execution_date, **kwargs):
        task_instance = kwargs['ti']
        live_uri = task_instance.xcom_pull(key='live_uri',task_ids='node_watch')
        with TemporaryDirectory() as tempdir:
            copy_from_export_path(
                export_path("contracts", execution_date), os.path.join(tempdir, "contracts.json")
            )

            logging.info('Calling filter_items(...)')
            filter_items.callback(
                input=os.path.join(tempdir, "contracts.json"),
                output=os.path.join(tempdir, "token_contracts.json"),
                predicate="item['is_erc20'] or item['is_erc721']",
            )

            logging.info('Removing unneeded file contracts.json')
            os.remove(os.path.join(tempdir, "contracts.json"))

            logging.info('Calling extract_field(...)')
            extract_field.callback(
                input=os.path.join(tempdir, "token_contracts.json"),
                output=os.path.join(tempdir, "token_addresses.txt"),
                field="address",
            )

            logging.info('Removing unneeded file token_contracts.json')
            os.remove(os.path.join(tempdir, "token_contracts.json"))

            logging.info('Calling export_tokens(..., {}, {})'.format(export_max_workers, live_uri))
            export_tokens.callback(
                token_addresses=os.path.join(tempdir, "token_addresses.txt"),
                output=os.path.join(tempdir, "tokens.csv"),
                max_workers=export_max_workers,
                provider_uri=live_uri,
            )

            copy_to_export_path(
                os.path.join(tempdir, "tokens.csv"), export_path("tokens", execution_date)
            )

    def extract_token_transfers_command(execution_date, **kwargs):
        with TemporaryDirectory() as tempdir:
            copy_from_export_path(
                export_path("logs", execution_date), os.path.join(tempdir, "logs.json")
            )

            logging.info('Calling extract_token_transfers(..., {}, ..., {})'.format(
                export_batch_size, export_max_workers
            ))
            extract_token_transfers.callback(
                logs=os.path.join(tempdir, "logs.json"),
                batch_size=export_batch_size,
                output=os.path.join(tempdir, "token_transfers.csv"),
                max_workers=export_max_workers,
            )

            copy_to_export_path(
                os.path.join(tempdir, "token_transfers.csv"),
                export_path("token_transfers", execution_date),
            )

    def export_traces_command(execution_date, **kwargs):
        task_instance = kwargs['ti']
        live_uri = task_instance.xcom_pull(key='live_uri',task_ids='node_watch')
        with TemporaryDirectory() as tempdir:
            start_block, end_block = get_block_range(tempdir, execution_date,provider_uri=live_uri)
            logging.info('Calling export_traces({}, {}, {}, ...,{}, {}, {}, {})'.format(
                start_block, end_block, export_batch_size, export_max_workers, live_uri,
                export_genesis_traces_option, export_daofork_traces_option
            ))
            export_traces.callback(
                start_block=start_block,
                end_block=end_block,
                batch_size=export_batch_size,
                output=os.path.join(tempdir, "traces.csv"),
                max_workers=export_max_workers,
                provider_uri=live_uri,
                genesis_traces=export_genesis_traces_option,
                daofork_traces=export_daofork_traces_option,
            )

            copy_to_export_path(
                os.path.join(tempdir, "traces.csv"), export_path("traces", execution_date)
            )


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


    node_watch_operator = add_export_task(
        True,
        "node_watch",
        node_watch_command,
    )

    export_blocks_and_transactions_operator = add_export_task(
        export_blocks_and_transactions_toggle,
        "export_blocks_and_transactions",
        export_blocks_and_transactions_command,
        dependencies=[node_watch_operator]
    )

    export_receipts_and_logs_operator = add_export_task(
        export_receipts_and_logs_toggle,
        "export_receipts_and_logs",
        export_receipts_and_logs_command,
        dependencies=[export_blocks_and_transactions_operator],
    )

    extract_token_transfers_operator = add_export_task(
        extract_token_transfers_toggle,
        "extract_token_transfers",
        extract_token_transfers_command,
        dependencies=[export_receipts_and_logs_operator],
    )

    export_traces_operator = add_export_task(
        export_traces_toggle, 
        "export_traces", 
        export_traces_command,
        dependencies=[node_watch_operator]
    )

    export_contracts_operator = add_export_task(
        export_contracts_toggle,
        "export_contracts",
        export_contracts_command,
        dependencies=[export_traces_operator],
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

    return dag


MEGABYTE = 1024 * 1024


# Helps avoid OverflowError: https://stackoverflow.com/questions/47610283/cant-upload-2gb-to-google-cloud-storage
# https://developers.google.com/api-client-library/python/guide/media_upload#resumable-media-chunked-upload
def upload_to_gcs(gcs_hook, bucket, object, filename, mime_type='application/octet-stream'):
    from apiclient.http import MediaFileUpload
    from googleapiclient import errors

    service = gcs_hook.get_conn()

    if os.path.getsize(filename) > 10 * MEGABYTE:
        media = MediaFileUpload(filename, mime_type, resumable=True)

        try:
            request = service.objects().insert(bucket=bucket, name=object, media_body=media)
            response = None
            while response is None:
                status, response = request.next_chunk()
                if status:
                    logging.info("Uploaded %d%%." % int(status.progress() * 100))

            return True
        except errors.HttpError as ex:
            if ex.resp['status'] == '404':
                return False
            raise
    else:
        media = MediaFileUpload(filename, mime_type)

        try:
            service.objects().insert(bucket=bucket, name=object, media_body=media).execute()
            return True
        except errors.HttpError as ex:
            if ex.resp['status'] == '404':
                return False
            raise


# Can download big files unlike gcs_hook.download which saves files in memory first
def download_from_gcs(bucket, object, filename):
    from google.cloud import storage

    storage_client = storage.Client()

    bucket = storage_client.get_bucket(bucket)
    blob_meta = bucket.get_blob(object)

    if blob_meta.size > 10 * MEGABYTE:
        blob = bucket.blob(object, chunk_size=10 * MEGABYTE)
    else:
        blob = bucket.blob(object)

    blob.download_to_filename(filename)

