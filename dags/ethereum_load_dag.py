from __future__ import print_function

import logging
from datetime import datetime
from airflow.models import Variable

from build_load_dag import build_load_dag
from build_load_dag_redshift import build_load_dag_redshift

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
start_date = Variable.get('ethereum_load_start_date', '2018-07-01')

cloud_provider = Variable.get('cloud_provider', 'gcp')

if cloud_provider == 'gcp':
    DAG = build_load_dag(
        dag_id='ethereum_load_dag',
        output_bucket=Variable.get('ethereum_output_bucket'),
        destination_dataset_project_id=Variable.get('destination_dataset_project_id'),
        chain='ethereum',
        notification_emails=Variable.get('notification_emails', ''),
        start_date=datetime.strptime(start_date, '%Y-%m-%d'),
        schedule_interval='30 8 * * *'
    )
elif cloud_provider == 'aws':
    DAG = build_load_dag_redshift(
        dag_id='ethereum_load_dag',
        output_bucket=Variable.get('ethereum_output_bucket'),
        aws_access_key_id=Variable.get('aws_access_key_id'),
        aws_secret_access_key=Variable.get('aws_secret_access_key'),
        chain='ethereum',
        notification_emails=Variable.get('notification_emails', ''),
        start_date=datetime.strptime(start_date, '%Y-%m-%d'),
        schedule_interval='30 8 * * *'
    )
else:
    raise ValueError('You must set a valid cloud_provider Airflow variable (gcp,aws)')
