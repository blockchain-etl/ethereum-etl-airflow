import csv
import json
import logging
import os
import time
from tempfile import TemporaryDirectory

import airflow
import requests
from airflow import DAG
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.operators import python_operator
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta, datetime

from blockchainetl.csv_utils import set_max_field_size_limit
from blockchainetl.file_utils import smart_open

from ethereumetl_airflow.abi import get_contract_code
from ethereumetl_airflow.variables import read_var

from ethereumetl.cli import (
    get_block_range_for_date,
    extract_csv_column,
    export_blocks_and_transactions,
    export_receipts_and_logs,
    extract_contracts,
    extract_tokens,
    extract_token_transfers,
    export_traces,
    extract_field)

default_dag_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime.strptime('2015-07-30', '%Y-%m-%d'),
    'email_on_failure': True,
}

notification_emails = os.environ.get('NOTIFICATION_EMAILS')
if notification_emails and len(notification_emails) > 0:
    default_dag_args['email'] = [email.strip() for email in notification_emails.split(',')]

output_bucket = read_var('output_bucket', 'ethereum_', True)

dag = DAG(
    'export_all_contract_codes_dag',
    default_args=default_dag_args,
    description='test',
    max_active_runs=3,
    schedule_interval='0 0 * * *')

MEGABYTE = 1024 * 1024


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


def copy_from_export_path(export_path, file_path):
    logging.info('Calling copy_from_export_path({}, {})'.format(export_path, file_path))
    filename = os.path.basename(file_path)
    download_from_gcs(bucket=output_bucket, object=export_path + filename, filename=file_path)


def export_path(directory, date):
    return "export/{directory}/block_date={block_date}/".format(
        directory=directory, block_date=date.strftime("%Y-%m-%d")
    )


cloud_storage_hook = GoogleCloudStorageHook(google_cloud_storage_conn_id="google_cloud_default")


def copy_to_export_path(file_path, export_path):
    logging.info('Calling copy_to_export_path({}, {})'.format(file_path, export_path))
    filename = os.path.basename(file_path)

    upload_to_gcs(
        gcs_hook=cloud_storage_hook,
        bucket=output_bucket,
        object=export_path + filename,
        filename=file_path)


def scrape_contract_codes_command(execution_date, **kwargs):
    with TemporaryDirectory() as tempdir:
        copy_from_export_path(
            export_path("contracts", execution_date), os.path.join(tempdir, "contracts.json")
        )

        set_max_field_size_limit()

        extract_field.callback(
            input=os.path.join(tempdir, "contracts.json"),
            output=os.path.join(tempdir, "contract_addresses.txt"),
            field="address",
        )

        with smart_open(os.path.join(tempdir, "contract_addresses.txt"), 'r') as addresses_file,\
                smart_open(os.path.join(tempdir, "contract_codes.csv"), 'w') as output_file:
            address_iterable = (address.strip() for address in addresses_file)
            writer = csv.DictWriter(output_file, fieldnames=['address', 'response'], extrasaction='ignore')

            for address in address_iterable:
                code = ''
                for i in range(1, 5):
                    try:
                        code = get_contract_code(address)
                        break
                    except Exception as e:
                        if i >= 4:
                            raise e
                        else:
                            logging.exception('An exception occurred while getting contract code')
                            time.sleep(1)
                writer.writerow({
                    'address': address,
                    'response': code
                })
                time.sleep(0.2)

        copy_to_export_path(
            os.path.join(tempdir, "contract_codes.csv"), export_path("contract_codes", execution_date)
        )


operator = python_operator.PythonOperator(
    task_id='scrape_codes',
    python_callable=scrape_contract_codes_command,
    provide_context=True,
    execution_timeout=timedelta(hours=168),
    dag=dag,
)
