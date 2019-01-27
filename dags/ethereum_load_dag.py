from __future__ import print_function

import logging
from datetime import datetime
from airflow.models import Variable

from build_load_dag import build_load_dag

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

DAG = build_load_dag(
    dag_id='ethereum_load_dag',
    output_bucket=Variable.get('ethereum_output_bucket'),
    destination_dataset_project_id=Variable.get('destination_dataset_project_id'),
    chain='ethereum',
    notification_emails=Variable.get('notification_emails', ''),
    start_date=datetime(2018, 7, 1),
    schedule_interval='30 8 * * *'
)
