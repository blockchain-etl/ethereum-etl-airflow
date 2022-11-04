import logging
import time

from ethereumetl_airflow.common import get_list_of_files, read_json_file
from ethereumetl_airflow.parse.parse_table_definition_logic import parse, ref_regex
from ethereumetl_airflow.parse.toposort import toposort_flatten


def parse_dataset_folder(
        bigquery_client,
        dataset_folder,
        ds,
        source_project_id,
        source_dataset_name,
        destination_project_id,
        sqls_folder,
        parse_all_partitions,
        time_func=time.time
):
    logging.info(f'Parsing dataset folder {dataset_folder}')
    json_files = get_list_of_files(dataset_folder, '*.json')
    logging.info(json_files)

    topologically_sorted_json_files = topologically_sort_json_files(json_files)
    logging.info(f'Topologically sorted json files: {topologically_sorted_json_files}')

    for index, json_file in enumerate(topologically_sorted_json_files):
        logging.info(f'Parsing json file {index} out of {len(topologically_sorted_json_files)}: {json_file}')
        table_definition = read_json_file(json_file)
        parse(
            bigquery_client,
            table_definition,
            ds,
            source_project_id,
            source_dataset_name,
            destination_project_id,
            sqls_folder,
            parse_all_partitions,
            time_func=time_func
        )


def topologically_sort_json_files(json_files):
    table_name_to_file_map = {}
    dependencies = {}

    for json_file in json_files:
        table_definition = read_json_file(json_file)

        contract_address = table_definition['parser']['contract_address']

        ref_dependencies = ref_regex.findall(contract_address) if contract_address is not None else None

        table_name = get_table_name_from_json_file_name(json_file)

        dependencies[table_name] = set(ref_dependencies) if ref_dependencies is not None else set()
        table_name_to_file_map[table_name] = json_file

    validate_dependencies(dependencies, table_name_to_file_map.keys())
    logging.info(f'Table definition dependencies: {dependencies}')

    # TODO: Use toposort() instead of toposort_flatten() so that independent tables could be run in parallel
    sorted_tables = list(toposort_flatten(dependencies))

    topologically_sorted_json_files = [table_name_to_file_map[table_name] for table_name in sorted_tables]
    return topologically_sorted_json_files


def validate_dependencies(dependencies, table_names):
    for deps in dependencies.values():
        for dep_table_name in deps:
            if dep_table_name not in table_names:
                raise ValueError(f'Dependency {dep_table_name} not found. Check ref() in table definitions')


def get_table_name_from_json_file_name(json_file_name):
    return json_file_name.split('/')[-1].replace('.json', '')