import logging
import os
import time

from ethereumetl_airflow.bigquery_utils import create_view
from ethereumetl_airflow.common import get_list_of_files, read_json_file, read_file
from ethereumetl_airflow.parse.parse_table_definition_logic import parse, ref_regex, create_dataset, replace_refs
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
    files = get_list_of_files(dataset_folder, ['*.json', '*.sql'])
    logging.info(files)

    topologically_sorted_files = topologically_sort_files(files)
    logging.info(f'Topologically sorted files: {topologically_sorted_files}')

    for index, file in enumerate(topologically_sorted_files):
        logging.info(f'Parsing file {index} out of {len(topologically_sorted_files)}: {file}')
        if '.json' in file:
            table_definition = read_json_file(file)
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
        elif '.sql' in file:
            base_name = os.path.basename(file)
            view_name = os.path.splitext(base_name)[0]
            dest_table_name = view_name

            dataset_name = os.path.basename(dataset_folder)
            dataset_name = 'ethereum_' + dataset_name

            dest_table_ref = create_dataset(bigquery_client, dataset_name, destination_project_id).table(dest_table_name)
            sql = read_file(file)
            sql = replace_refs(sql, ref_regex, destination_project_id, dataset_name)

            create_view(bigquery_client, sql, dest_table_ref)
        else:
            raise ValueError(f'Unrecognized file type: {file}')


def topologically_sort_files(files):
    table_name_to_file_map = {}
    dependencies = {}

    for file in files:
        ref_dependencies = None
        if '.json' in file:
            table_definition = read_json_file(file)
            contract_address = table_definition['parser']['contract_address']
            ref_dependencies = ref_regex.findall(contract_address) if contract_address is not None else None
        elif '.sql' in file:
            sql = read_file(file)
            ref_dependencies = ref_regex.findall(sql) if sql is not None else None

        table_name = get_table_name_from_file_name(file)

        dependencies[table_name] = set(ref_dependencies) if ref_dependencies is not None else set()
        table_name_to_file_map[table_name] = file

    validate_dependencies(dependencies, table_name_to_file_map.keys())
    logging.info(f'Table definition dependencies: {dependencies}')

    # TODO: Use toposort() instead of toposort_flatten() so that independent tables could be run in parallel
    sorted_tables = list(toposort_flatten(dependencies))

    topologically_sorted_files = [table_name_to_file_map[table_name] for table_name in sorted_tables]
    return topologically_sorted_files


def validate_dependencies(dependencies, table_names):
    for deps in dependencies.values():
        for dep_table_name in deps:
            if dep_table_name not in table_names:
                raise ValueError(f'Dependency {dep_table_name} not found. Check ref() in table definitions')


def get_table_name_from_file_name(file_name):
    return file_name.split('/')[-1].replace('.json', '').replace('.sql', '')