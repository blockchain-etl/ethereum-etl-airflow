from __future__ import print_function

import logging
from datetime import datetime, timedelta

from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import ExternalTaskSensor
from google.cloud import bigquery

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)


def build_partition_dag(
    dag_id,
    notification_emails=None,
    schedule_interval='0 0 * * *'
):

    default_dag_args = {
        'depends_on_past': False,
        'start_date': datetime.strptime('2015-07-30', '%Y-%m-%d'),
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 5,
        'retry_delay': timedelta(minutes=5)
    }

    if notification_emails and len(notification_emails) > 0:
        default_dag_args['email'] = [email.strip() for email in notification_emails.split(',')]

    # Define a DAG (directed acyclic graph) of tasks.
    dag = models.DAG(
        dag_id,
        catchup=False,
        schedule_interval=schedule_interval,
        default_args=default_dag_args)

    def add_partition_tasks(task, sql_template, dependencies=None):
        def enrich_task(ds, **kwargs):
            client = bigquery.Client()

            ds_with_underscores = ds.replace('-', '_')
            sql = sql_template.format(ds=ds, ds_with_underscores=ds_with_underscores)
            print(sql)
            query_job = client.query(sql)
            result = query_job.result()
            logging.info(result)

        extract_operator = PythonOperator(
            task_id=f'partition_{task}',
            python_callable=enrich_task,
            provide_context=True,
            execution_timeout=timedelta(minutes=60),
            dag=dag
        )

        if dependencies is not None and len(dependencies) > 0:
            for dependency in dependencies:
                dependency >> extract_operator
        return extract_operator

    wait_for_ethereum_load_dag_task = ExternalTaskSensor(
        task_id='wait_for_ethereum_load_dag',
        external_dag_id='ethereum_load_dag',
        external_task_id='verify_logs_have_latest',
        execution_delta=timedelta(hours=1),
        priority_weight=0,
        mode='reschedule',
        poke_interval=5 * 60,
        timeout=60 * 60 * 12,
        dag=dag)

    partition_logs_task = add_partition_tasks('logs', SQL_TEMPLATE_LOGS)
    partition_traces_task = add_partition_tasks('traces', SQL_TEMPLATE_TRACES)
    partition_balances_task = add_partition_tasks('balances', SQL_TEMPLATE_BALANCES)

    wait_for_ethereum_load_dag_task >> partition_logs_task
    wait_for_ethereum_load_dag_task >> partition_traces_task
    wait_for_ethereum_load_dag_task >> partition_balances_task

    done_task = BashOperator(
        task_id='done',
        bash_command='echo done',
        dag=dag
    )

    partition_logs_task >> done_task
    partition_traces_task >> done_task
    partition_balances_task >> done_task

    return dag


SQL_TEMPLATE_LOGS = '''
CREATE OR REPLACE TABLE
  `blockchain-etl-internal.crypto_ethereum_partitioned.logs_by_date_{ds_with_underscores}`
PARTITION BY
  RANGE_BUCKET(_topic_partition_index, GENERATE_ARRAY(0, 3999, 1))
OPTIONS(
  expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 72 HOUR)
)
AS

SELECT 
  MOD(ABS(FARM_FINGERPRINT(topics[SAFE_OFFSET(0)])), 3999) as _topic_partition_index, 
  *
FROM `bigquery-public-data.crypto_ethereum.logs`
WHERE date(block_timestamp) = '{ds}'
'''

SQL_TEMPLATE_TRACES = '''
CREATE OR REPLACE TABLE
  `blockchain-etl-internal.crypto_ethereum_partitioned.traces_by_date_{ds_with_underscores}`
PARTITION BY
  RANGE_BUCKET(_input_partition_index, GENERATE_ARRAY(0, 3999, 1))
OPTIONS(
  expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 72 HOUR)
)
AS

SELECT 
  MOD(ABS(FARM_FINGERPRINT(SUBSTRING(input, 0, 10))), 3999) as _input_partition_index, 
  *
FROM `bigquery-public-data.crypto_ethereum.traces`
WHERE date(block_timestamp) = '{ds}'
'''

SQL_TEMPLATE_BALANCES = '''
CREATE OR REPLACE TABLE
  `blockchain-etl-internal.common.ethereum_balances_{ds_with_underscores}`
OPTIONS(
  expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
)
AS

SELECT *
FROM `bigquery-public-data.crypto_ethereum.balances`;

CREATE OR REPLACE VIEW
  `blockchain-etl-internal.common.ethereum_balances_live`
AS

with latest_double_entry_book as (
    -- debits
    select to_address as address, value as value
    from `bigquery-public-data.crypto_ethereum.traces`
    where true
    and date(block_timestamp) > '{ds}'
    and to_address is not null
    and status = 1
    and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
    union all
    -- credits
    select from_address as address, -value as value
    from `bigquery-public-data.crypto_ethereum.traces`
    where true
    and date(block_timestamp) > '{ds}'
    and from_address is not null
    and status = 1
    and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
    union all
    -- transaction fees debits
    select miner as address, sum(cast(receipt_gas_used as numeric) * cast(gas_price as numeric)) as value
    from `bigquery-public-data.crypto_ethereum.transactions` as transactions
    join `bigquery-public-data.crypto_ethereum.blocks` as blocks on blocks.number = transactions.block_number
    where true
    and date(transactions.block_timestamp) > '{ds}'
    group by blocks.miner
    union all
    -- transaction fees credits
    select from_address as address, -(cast(receipt_gas_used as numeric) * cast(gas_price as numeric)) as value
    from `bigquery-public-data.crypto_ethereum.transactions`
    where true
    and date(block_timestamp) > '{ds}'
),
latest_balance_changes as (
  select address, sum(value) as eth_change
  from latest_double_entry_book
  group by address
)
select address, coalesce(sum(eth_balance), 0) + coalesce(sum(eth_change), 0) as eth_balance
from `blockchain-etl-internal.common.ethereum_balances_{ds_with_underscores}` as historical_balances
full outer join latest_balance_changes using(address)
group by address;
'''