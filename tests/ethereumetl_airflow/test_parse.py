import io
import os
from datetime import timedelta

import airflow
import pytest
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from ethereumetl_airflow.common import read_json_file
from ethereumetl_airflow.parse import create_or_update_table_from_table_definition
from tests.ethereumetl_airflow.mock_bigquery_client import MockBigqueryClient


sqls_folder = 'dags/resources/stages/parse/sqls'
table_definitions_folder = 'dags/resources/stages/parse/table_definitions'

@pytest.mark.parametrize("table_definition_file", [
    ('ens/Registrar0_event_NewBid.json'),
    ('uniswap/Uniswap_event_AddLiquidity.json'),
])
def test_create_or_update_table_from_table_definition(table_definition_file):
    bigquery_client = MockBigqueryClient()
    table_definition = read_json_file(os.path.join(table_definitions_folder, table_definition_file))

    create_or_update_table_from_table_definition(
        bigquery_client=bigquery_client,
        table_definition=table_definition,
        ds='2020-01-01',
        source_project_id='bigquery-public-data',
        source_dataset_name='crypto_ethereum',
        destination_project_id='blockchain-etl',
        sqls_folder=sqls_folder,
        parse_all_partitions=True,
        airflow_task=create_dummy_airflow_task()
    )

    assert len(bigquery_client.queries) == 1
    expected_filename = table_definition_file_to_expected_file(table_definition_file)
    assert trim(bigquery_client.queries[0]) == trim(read_resource(expected_filename))


def table_definition_file_to_expected_file(table_definition_file):
    return 'expected_' + table_definition_file.replace('/', '_') + '.sql'


def create_dummy_airflow_task():
    default_dag_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'start_date': airflow.utils.dates.days_ago(0),
        'email_on_failure': True,
    }

    dummy_dag = DAG(
        'dummy_dag',
        default_args=default_dag_args,
        description='dummy_dag')

    dummy_task = BashOperator(task_id='echo', bash_command='echo test', dag=dummy_dag, depends_on_past=False)

    return dummy_task


def read_resource(filename):
    full_filepath = 'tests/resources/ethereumetl_airflow/test_parse/' + filename
    return open(full_filepath).read()


def trim(content):
    stripped_lines = [line.strip() for line in io.StringIO(content)]
    return '\n'.join(stripped_lines)