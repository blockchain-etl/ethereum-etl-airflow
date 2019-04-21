import csv
import json
import logging
import os
import time
from datetime import timedelta, datetime
from tempfile import TemporaryDirectory

from airflow import DAG
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.operators import python_operator
from airflow.operators.python_operator import PythonOperator
from blockchainetl.file_utils import smart_open
from google.cloud import bigquery

from ethereumetl_airflow.abi import get_contract_abi
from ethereumetl_airflow.variables import read_var

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
    'export_popular_contract_abis_dag',
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


def scrape_contract_abis_command(execution_date, **kwargs):
    with TemporaryDirectory() as tempdir:
        copy_from_export_path(
            export_path("popular_contracts", execution_date), os.path.join(tempdir, "popular_contracts.csv")
        )

        with smart_open(os.path.join(tempdir, "popular_contracts.csv"), 'r') as addresses_file, \
                smart_open(os.path.join(tempdir, "popular_contract_abis.csv"), 'w') as output_file:
            address_iterable = (address.strip() for address in addresses_file)
            writer = csv.DictWriter(output_file, fieldnames=['address', 'abi'], extrasaction='ignore')

            for address in address_iterable:
                abi = ''
                for i in range(1, 5):
                    try:
                        abi = get_contract_abi(address)
                        break
                    except Exception as e:
                        if i >= 4:
                            raise e
                        else:
                            logging.exception('An exception occurred while getting contract abi')
                            time.sleep(1)
                writer.writerow({
                    'address': address,
                    'abi': abi
                })
                time.sleep(0.2)

        copy_to_export_path(
            os.path.join(tempdir, "popular_contract_abis.csv"), export_path("popular_contract_abis", execution_date)
        )


def submit_bigquery_job(job, configuration):
    try:
        logging.info('Creating a job: ' + json.dumps(configuration.to_api_repr()))
        result = job.result()
        logging.info(result)
        assert job.errors is None or len(job.errors) == 0
        return result
    except Exception:
        logging.info(job.errors)
        raise


def add_delete_partition_operator():
    def delete_task(ds, **kwargs):
        client = bigquery.Client()

        dest_table_ref = client.dataset('graph_database_test_temp').table(
            'popular_contracts_{}'.format(ds.replace('-', '')))
        # Delete temp table
        client.delete_table(dest_table_ref)

    operator = PythonOperator(
        task_id='delete_partition',
        python_callable=delete_task,
        provide_context=True,
        execution_timeout=timedelta(minutes=60),
        dag=dag
    )

    return operator


def add_copy_partition_operator():
    def copy_task(ds, **kwargs):
        client = bigquery.Client()

        dest_table_ref = client.dataset('graph_database_test_temp').table(
            'popular_contracts_{}'.format(ds.replace('-', '')))
        # Query from raw to temporary table
        query_job_config = bigquery.QueryJobConfig()
        # Finishes faster, query limit for concurrent interactive queries is 50
        query_job_config.priority = bigquery.QueryPriority.INTERACTIVE
        query_job_config.destination = dest_table_ref
        query_job_config.write_disposition = 'WRITE_TRUNCATE'

        sql = "select address from graph_database_test.popular_contracts where date(block_timestamp) = '{}'".format(ds)
        print(sql)

        query_job = client.query(sql, location='US', job_config=query_job_config)
        submit_bigquery_job(query_job, query_job_config)
        assert query_job.state == 'DONE'

    operator = PythonOperator(
        task_id='copy_partition',
        python_callable=copy_task,
        provide_context=True,
        execution_timeout=timedelta(minutes=60),
        dag=dag
    )

    return operator


copy_operator = add_copy_partition_operator()

export_operator = BigQueryToCloudStorageOperator(
    task_id='export_popular_contracts',
    source_project_dataset_table='graph_database_test_temp.popular_contracts_{{ ds_nodash }}',
    destination_cloud_storage_uris=[
        'gs://ethereum-etl-dev/export/popular_contracts/block_date={{ ds }}/popular_contracts.csv'],
    export_format='CSV',
    dag=dag
)

scrape_operator = python_operator.PythonOperator(
    task_id='scrape_abis',
    python_callable=scrape_contract_abis_command,
    provide_context=True,
    execution_timeout=timedelta(hours=2),
    dag=dag,
)

delete_operator = add_delete_partition_operator()

copy_operator >> export_operator >> scrape_operator >> delete_operator
