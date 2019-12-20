from __future__ import print_function

import logging
import os
import random
import time
from datetime import datetime, timedelta
from tempfile import TemporaryDirectory
import csv

from airflow import DAG
from airflow.operators import python_operator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

from blockchainetl.csv_utils import set_max_field_size_limit
from blockchainetl.file_utils import smart_open
from ethereumetl.cli import extract_field

from ethereumetl_airflow.etherscan import get_contract_code
from ethereumetl_airflow.gcs_utils import download_from_gcs, upload_to_gcs

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)


def build_scrape_etherscan_contracts_dag(
        dag_id,
        input_bucket,
        output_bucket,
        etherscan_api_tokens,
        notification_emails=None,
        scrape_start_date=datetime(2015, 7, 1),
        scrape_schedule_interval='0 13 * * *'):

    default_dag_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'start_date': scrape_start_date,
        'email_on_failure': True,
    }

    if notification_emails and len(notification_emails) > 0:
        default_dag_args['email'] = [email.strip() for email in notification_emails.split(',')]

    dag = DAG(
        dag_id,
        default_args=default_dag_args,
        description='Scraping of contracts from Etherscan',
        max_active_runs=3,
        schedule_interval=scrape_schedule_interval)

    wait_sensor = GoogleCloudStorageObjectSensor(
        task_id='wait_latest_contracts',
        timeout=60 * 60,
        poke_interval=60,
        bucket=input_bucket,
        object="export/contracts/block_date={{ds}}/contracts.json",
        dag=dag
    )

    def scrape_contract_codes_command(execution_date, **kwargs):
        with TemporaryDirectory() as tempdir:
            copy_from_export_path(
                input_bucket, export_path("contracts", execution_date), os.path.join(tempdir, "contracts.json")
            )

            set_max_field_size_limit()

            extract_field.callback(
                input=os.path.join(tempdir, "contracts.json"),
                output=os.path.join(tempdir, "contract_addresses.txt"),
                field="address",
            )

            with smart_open(os.path.join(tempdir, "contract_addresses.txt"), 'r') as addresses_file, \
                    smart_open(os.path.join(tempdir, "contract_codes.csv"), 'w') as output_file:
                address_iterable = (address.strip() for address in addresses_file)
                writer = csv.DictWriter(output_file, fieldnames=['address', 'response'], extrasaction='ignore')

                for address in address_iterable:
                    code = ''
                    for i in range(1, 5):
                        try:
                            code = get_contract_code(address, random.choice(etherscan_api_tokens))
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
                output_bucket, os.path.join(tempdir, "contract_codes.csv"), export_path("contract_codes", execution_date)
            )

    scrape_contracts_operator = python_operator.PythonOperator(
        task_id='scrape_contracts',
        python_callable=scrape_contract_codes_command,
        provide_context=True,
        execution_timeout=timedelta(hours=168),
        dag=dag,
    )

    wait_sensor >> scrape_contracts_operator


def copy_from_export_path(bucket, export_path, file_path):
    logging.info('Calling copy_from_export_path({}, {})'.format(export_path, file_path))
    filename = os.path.basename(file_path)
    download_from_gcs(bucket=bucket, object=export_path + filename, filename=file_path)


def export_path(directory, date):
    return "export/{directory}/block_date={block_date}/".format(
        directory=directory, block_date=date.strftime("%Y-%m-%d")
    )


cloud_storage_hook = GoogleCloudStorageHook(google_cloud_storage_conn_id="google_cloud_default")


def copy_to_export_path(bucket, file_path, export_path):
    logging.info('Calling copy_to_export_path({}, {})'.format(file_path, export_path))
    filename = os.path.basename(file_path)

    upload_to_gcs(
        gcs_hook=cloud_storage_hook,
        bucket=bucket,
        object=export_path + filename,
        filename=file_path)
