from __future__ import print_function

from datetime import datetime

from airflow.models import Variable

from ethereumetl.build_export_dag import build_export_dag

start_date = Variable.get('ethereum_classic_export_start_date', '2015-07-30')

DAG = build_export_dag(
    dag_id='ethereum_classic_export_dag',
    provider_uri=Variable.get('ethereum_classic_provider_uri'),
    output_bucket=Variable.get('ethereum_classic_output_bucket'),
    start_date=datetime.strptime(start_date, '%Y-%m-%d'),
    chain='ethereum_classic',
    notifications_emails=Variable.get('notification_emails', ''),
    schedule_interval='0 8 * * *',
    export_max_workers=10,
    export_batch_size=10
)
 
