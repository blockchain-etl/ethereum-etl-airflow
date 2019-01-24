from __future__ import print_function

import logging

from airflow.models import Variable

from ethereumetl.build_load_dag import build_load_dag

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

DAG = build_load_dag(
    dag_id='ethereum_load_dag',
    output_bucket=Variable.get('ethereum_output_bucket'),
    destination_dataset_project_id=Variable.get('destination_dataset_project_id'),
    chain='ethereum',
    notification_emails=Variable.get('notification_emails', ''),
    schedule_interval='30 8 * * *'
)
