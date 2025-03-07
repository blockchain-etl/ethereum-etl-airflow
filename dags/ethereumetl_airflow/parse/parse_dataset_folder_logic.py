import logging
import os
import time

from ethereumetl_airflow.bigquery_utils import create_view
from ethereumetl_airflow.common import read_file, read_json_file
from ethereumetl_airflow.parse.parse_state_manager import ParseStateManager
from ethereumetl_airflow.parse.parse_table_definition_logic import (
    create_dataset,
    parse,
    ref_regex,
    replace_refs,
)
from ethereumetl_airflow.parse.table_definition import TableDefinitionFileType
from ethereumetl_airflow.parse.table_definition_reader import (
    read_table_definitions,
    toposort_and_read_table_definition_states,
)


def parse_dataset_folder(
    bigquery_client,
    dataset_folder,
    ds,
    source_project_id,
    source_dataset_name,
    destination_project_id,
    parse_state_manager: ParseStateManager,
    sqls_folder,
    parse_all_partitions,
    time_func=time.time,
    only_updated=False,
):
    logging.info(f"Parsing dataset folder {dataset_folder}")

    if ds is None:
        # If in CI/CD pipeline get ds from state
        if parse_state_manager.is_state_empty:
            ds = parse_state_manager.get_fallback_last_ds()
        else:
            ds = parse_state_manager.get_last_ds()
    else:
        if parse_state_manager.is_state_empty:
            sleep_seconds = 1200
            # Hacky way to prevent a race condition with CI/CD pipeline
            logging.info(
                f"Sleeping for {sleep_seconds} seconds to prevent race condition with CI/CD pipeline"
            )
            time.sleep(sleep_seconds)
            parse_state_manager.sync_state_file()

    table_definitions = read_table_definitions(dataset_folder)

    table_definition_states = toposort_and_read_table_definition_states(
        table_definitions, parse_state_manager
    )

    for table_definition_state in table_definition_states:
        logging.info(
            f"{table_definition_state.table_definition.table_name}: is_updated_or_dependencies_updated: {table_definition_state.is_updated_or_dependencies_updated}"
        )

    updated_table_definitions = [
        tds.table_definition
        for tds in table_definition_states
        if tds.is_updated_or_dependencies_updated
    ]

    # Prevents accidentally running full refresh for many tables caused by bugs
    max_num_updated_table_definitions = 70
    num_updated_table_definitions = len(updated_table_definitions)
    if num_updated_table_definitions > max_num_updated_table_definitions:
        raise ValueError(
            f"{num_updated_table_definitions} will be fully refreshed. Please make sure not more than {max_num_updated_table_definitions} are updated"
        )

    for index, table_definition_state in enumerate(table_definition_states):
        table_definition = table_definition_state.table_definition
        if (
            only_updated
            and not table_definition_state.is_updated_or_dependencies_updated
        ):
            logging.info(
                f"Skipping file {index} out of {len(table_definition_states)}: {table_definition.filepath}"
            )
            continue

        logging.info(
            f"Parsing file {index} out of {len(table_definition_states)}: {table_definition.filepath}"
        )
        if table_definition.filetype == TableDefinitionFileType.JSON:
            table_definition_content = read_json_file(table_definition.filepath)
            parse(
                bigquery_client,
                table_definition_content,
                ds,
                source_project_id,
                source_dataset_name,
                destination_project_id,
                sqls_folder,
                (
                    parse_all_partitions
                    if parse_all_partitions is not None
                    else table_definition_state.is_updated_or_dependencies_updated
                ),
                time_func=time_func,
            )
        elif table_definition.filetype == TableDefinitionFileType.SQL:
            view_name = table_definition.table_name
            dest_table_name = view_name

            dataset_name = os.path.basename(dataset_folder)
            dataset_name = "ethereum_" + dataset_name

            dest_table_ref = create_dataset(
                bigquery_client, dataset_name, destination_project_id
            ).table(dest_table_name)
            sql = read_file(table_definition.filepath)
            sql = replace_refs(sql, ref_regex, destination_project_id, dataset_name)

            create_view(bigquery_client, sql, dest_table_ref)
        else:
            raise ValueError(f"Unrecognized file type: {table_definition.filepath}")

    for table_definition_state in table_definition_states:
        if table_definition_state.is_updated_or_dependencies_updated:
            table_definition = table_definition_state.table_definition
            logging.info(
                f"Updating content hash for {table_definition.table_name}: {table_definition.content_hash}"
            )
            parse_state_manager.set_content_hash(
                table_definition.table_name, table_definition.content_hash
            )

    parse_state_manager.set_last_ds(ds)
    parse_state_manager.persist_state()
