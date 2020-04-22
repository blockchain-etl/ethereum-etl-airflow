import io
import os

import pytest

from ethereumetl_airflow.common import read_json_file
from ethereumetl_airflow.parse import create_or_update_table_from_table_definition
from tests.ethereumetl_airflow.mock_bigquery_client import MockBigqueryClient

sqls_folder = 'dags/resources/stages/parse/sqls'
table_definitions_folder = 'dags/resources/stages/parse/table_definitions'


@pytest.mark.parametrize("table_definition_file", [
    ('ens/Registrar0_event_NewBid.json'),
    ('uniswap/Uniswap_event_AddLiquidity.json'),
    ('dydx/SoloMargin_event_LogTrade.json'),
    ('idex/Exchange_call_trade.json'),
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
    )

    assert len(bigquery_client.queries) == 1
    expected_filename = table_definition_file_to_expected_file(table_definition_file)
    assert trim(bigquery_client.queries[0]) == trim(read_resource(expected_filename))


def table_definition_file_to_expected_file(table_definition_file):
    return 'expected_' + table_definition_file.replace('/', '_') + '.sql'


def read_resource(filename):
    full_filepath = 'tests/resources/ethereumetl_airflow/test_parse/' + filename
    return open(full_filepath).read()


def trim(content):
    stripped_lines = [line.strip() for line in io.StringIO(content)]
    return '\n'.join(stripped_lines)
