import hashlib
import json
from typing import List

from ethereumetl_airflow.common import get_list_of_files, read_file
from ethereumetl_airflow.parse.parse_state_manager import ParseStateManager
from ethereumetl_airflow.parse.parse_table_definition_logic import ref_regex
from ethereumetl_airflow.parse.table_definition import TableDefinition, TableDefinitionFileType
from ethereumetl_airflow.parse.table_definition_state import TableDefinitionState
from ethereumetl_airflow.parse.toposort import toposort_flatten


# Reads table definitions from a dataset_folder and returns a list of TableDefinition objects
def read_table_definitions(dataset_folder: str) -> List[TableDefinition]:
    file_paths = get_list_of_files(dataset_folder, ['*.json', '*.sql'])

    table_definitions_dict = {}
    for file_path in file_paths:
        table_name = extract_table_name(file_path)

        if table_definitions_dict.get(table_name):
            raise ValueError(f"Duplicate table name {table_name}")

        file_content = read_file(file_path)
        ref_dependencies, filetype = extract_file_dependencies_and_type(file_path, file_content)

        table_definition = TableDefinition(
            table_name=table_name,
            filetype=filetype,
            filepath=file_path,
            content_hash=calculate_content_hash(file_content),
            ref_dependencies=ref_dependencies,
            dependencies=None
        )
        table_definitions_dict[table_name] = table_definition

    validate_ref_dependencies(table_definitions_dict)
    set_table_dependencies(table_definitions_dict)

    return list(table_definitions_dict.values())


# Reads and returns the states of given table definitions
def toposort_and_read_table_definition_states(
        table_definitions: List[TableDefinition],
        parse_state_manager: ParseStateManager
) -> List[TableDefinitionState]:
    states = {}
    topologically_sorted_definitions = sort_table_definitions_topologically(table_definitions)

    for table_definition in topologically_sorted_definitions:
        previous_hash = parse_state_manager.get_content_hash(table_definition.table_name)
        current_hash = table_definition.content_hash
        is_updated = previous_hash != current_hash

        assert all(states.get(parent.table_name) is not None for parent in table_definition.dependencies)
        is_dependencies_updated = any(states[parent.table_name].is_updated_or_dependencies_updated for parent in table_definition.dependencies)
        is_updated_or_dependencies_updated = is_updated or is_dependencies_updated
        state = TableDefinitionState(table_definition, is_updated_or_dependencies_updated)
        states[table_definition.table_name] = state

    return [states[table_definition.table_name] for table_definition in topologically_sorted_definitions]


# Extracts table name from file path
def extract_table_name(file_path):
    return file_path.split('/')[-1].replace('.json', '').replace('.sql', '')


# Extracts dependencies and file type from file path and content
def extract_file_dependencies_and_type(file_path, file_content):
    if '.json' in file_path:
        filetype = TableDefinitionFileType.JSON
        parsed_content = json.loads(file_content)
        contract_address = parsed_content['parser']['contract_address']
        ref_dependencies = ref_regex.findall(contract_address) if contract_address is not None else []
    elif '.sql' in file_path:
        filetype = TableDefinitionFileType.SQL
        ref_dependencies = ref_regex.findall(file_content) if file_content is not None else []
    else:
        raise ValueError(f'Unknown file type {file_path}')
    return ref_dependencies, filetype


def validate_ref_dependencies(table_definitions_dict):
    for table_definition in table_definitions_dict.values():
        for dep_table_name in (table_definition.ref_dependencies or []):
            if dep_table_name not in table_definitions_dict:
                raise ValueError(f'Dependency {dep_table_name} not found. Check ref() in table definitions')


# Sets dependencies for each table in table_definitions_dict
def set_table_dependencies(table_definitions_dict):
    for table_definition in table_definitions_dict.values():
        table_definition.dependencies = [table_definitions_dict[d] for d in table_definition.ref_dependencies]


# Returns a list of TableDefinition objects sorted topologically
def sort_table_definitions_topologically(table_definitions: List[TableDefinition]) -> List[TableDefinition]:
    table_name_to_definition_map = {}
    dependencies = {}

    for table_definition in table_definitions:
        table_dependencies = [d.table_name for d in table_definition.dependencies] if table_definition.dependencies else set()
        dependencies[table_definition.table_name] = table_dependencies
        table_name_to_definition_map[table_definition.table_name] = table_definition

    sorted_table_names = list(toposort_flatten(dependencies))
    return [table_name_to_definition_map[table_name] for table_name in sorted_table_names]


# Calculates and returns hash of the content
# Can be calculated from contents of a file in command line on MacOS:
# > shasum -a 256 <path_to_file>
def calculate_content_hash(content):
    hash_object = hashlib.sha256()
    hash_object.update(content.encode())
    return hash_object.hexdigest()
