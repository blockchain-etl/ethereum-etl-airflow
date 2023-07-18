import logging
import os

import pytest

from ethereumetl_airflow.parse.table_definition_reader import read_table_definitions, toposort_and_read_table_definition_states
from tests.ethereumetl_airflow.mock_parse_state_manager import MockParseStateManager

table_definitions_folder = 'dags/resources/stages/parse/table_definitions'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s [%(levelname)s] - %(message)s')

@pytest.mark.parametrize("dataset_folder", [
    ('chainlink'),
])
def test_read_table_definition_states(dataset_folder):
    full_dataset_folder = os.path.join(table_definitions_folder, dataset_folder)

    parse_state_manager = MockParseStateManager(full_dataset_folder, updated_table_names=('view_AccessControlledOffchainAggregator_info'))
    # parse_state_manager = ParseStateManager(dataset_folder, 'ethereum-etl-dev-2', 'parse/state')

    table_definitions = read_table_definitions(full_dataset_folder)

    table_definition_states = toposort_and_read_table_definition_states(table_definitions, parse_state_manager)

    updated_table_definitions = sorted([tds.table_definition.table_name for tds in table_definition_states if tds.is_updated_or_dependencies_updated])

    assert updated_table_definitions == [
        'AccessControlledOffchainAggregator_event_AnswerUpdated',
        'EACAggregatorProxy_call_confirmAggregator',
        'view_AccessControlledOffchainAggregator_info'
    ]

