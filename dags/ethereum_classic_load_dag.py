from __future__ import print_function

import logging
from datetime import datetime

from airflow.models import Variable

from build_load_dag import build_load_dag

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

start_date = Variable.get('ethereum_classic_load_start_date', '2018-07-01')

DAG = build_load_dag(
    dag_id='ethereum_classic_load_dag',
    output_bucket=Variable.get('ethereum_classic_output_bucket'),
    destination_dataset_project_id=Variable.get('destination_dataset_project_id'),
    chain='classic',
    notification_emails=Variable.get('notification_emails', ''),
    start_date=datetime.strptime(start_date, '%Y-%m-%d'),
    schedule_interval='30 8 * * *'
)
