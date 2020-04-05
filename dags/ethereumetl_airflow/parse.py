import json
import logging
import os
import re
import time

from eth_utils import event_abi_to_log_topic, function_abi_to_4byte_selector
from google.cloud import bigquery

from google.api_core.exceptions import Conflict

from ethereumetl_airflow.bigquery_utils import submit_bigquery_job, read_bigquery_schema_from_json_recursive

ref_regex = re.compile(r"ref\(\'([^']+)\'\)")


def create_or_update_table_from_table_definition(
        bigquery_client,
        table_definition,
        ds,
        source_project_id,
        source_dataset_name,
        destination_project_id,
        sqls_folder,
        parse_all_partitions,
        airflow_task
):
    dataset_name = 'ethereum_' + table_definition['table']['dataset_name']
    table_name = table_definition['table']['table_name']
    table_description = table_definition['table']['table_description']
    schema = table_definition['table']['schema']
    parser = table_definition['parser']
    parser_type = parser.get('type', 'log')
    abi = json.dumps(parser['abi'])
    columns = [c.get('name') for c in schema]

    template_context = {}
    template_context['ds'] = ds
    template_context['params'] = {}
    template_context['params']['source_project_id'] = source_project_id
    template_context['params']['source_dataset_name'] = source_dataset_name
    template_context['params']['table_name'] = table_name
    template_context['params']['columns'] = columns
    template_context['params']['parser'] = parser
    template_context['params']['abi'] = abi
    if parser_type == 'log':
        template_context['params']['event_topic'] = abi_to_event_topic(parser['abi'])
    elif parser_type == 'trace':
        template_context['params']['method_selector'] = abi_to_method_selector(parser['abi'])
    template_context['params']['struct_fields'] = create_struct_string_from_schema(schema)
    template_context['params']['parse_all_partitions'] = parse_all_partitions

    contract_address = parser['contract_address']
    if not contract_address.startswith('0x'):
        contract_address_sql = replace_refs(contract_address, ref_regex, destination_project_id,
                                            dataset_name)
        template_context['params']['parser']['contract_address_sql'] = contract_address_sql

    # # # Create a temporary table

    dataset_name_temp = 'parse_temp'
    create_dataset(bigquery_client, dataset_name_temp)
    temp_table_name = 'temp_{table_name}_{milliseconds}' \
        .format(table_name=table_name, milliseconds=int(round(time.time() * 1000)))
    temp_table_ref = bigquery_client.dataset(dataset_name_temp).table(temp_table_name)

    temp_table = bigquery.Table(temp_table_ref, schema=read_bigquery_schema_from_dict(schema, parser_type))

    temp_table.description = table_description
    temp_table.time_partitioning = bigquery.TimePartitioning(field='block_timestamp')
    logging.info('Creating table: ' + json.dumps(temp_table.to_api_repr()))
    temp_table = bigquery_client.create_table(temp_table)
    assert temp_table.table_id == temp_table_name

    # # # Query to temporary table

    job_config = bigquery.QueryJobConfig()
    job_config.priority = bigquery.QueryPriority.INTERACTIVE
    job_config.destination = temp_table_ref
    sql_template = get_parse_sql_template(parser_type, sqls_folder)
    sql = airflow_task.render_template('', sql_template, template_context)
    logging.info(sql)
    query_job = bigquery_client.query(sql, location='US', job_config=job_config)
    submit_bigquery_job(query_job, job_config)
    assert query_job.state == 'DONE'

    # # # Copy / merge to destination

    if parse_all_partitions:
        # Copy temporary table to destination
        copy_job_config = bigquery.CopyJobConfig()
        copy_job_config.write_disposition = 'WRITE_TRUNCATE'
        dataset = create_dataset(bigquery_client, dataset_name, destination_project_id)
        dest_table_ref = dataset.table(table_name)
        copy_job = bigquery_client.copy_table(temp_table_ref, dest_table_ref, location='US', job_config=copy_job_config)
        submit_bigquery_job(copy_job, copy_job_config)
        assert copy_job.state == 'DONE'
        # Need to do update description as copy above won't repect the description in case destination table
        # already exists
        table = bigquery_client.get_table(dest_table_ref)
        table.description = table_description
        table = bigquery_client.update_table(table, ["description"])
        assert table.description == table_description
    else:
        # Merge
        # https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement
        merge_job_config = bigquery.QueryJobConfig()
        # Finishes faster, query limit for concurrent interactive queries is 50
        merge_job_config.priority = bigquery.QueryPriority.INTERACTIVE

        merge_sql_template = get_merge_table_sql_template(sqls_folder)
        merge_template_context = template_context.copy()
        merge_template_context['params']['source_table'] = temp_table_name
        merge_template_context['params']['destination_dataset_project_id'] = destination_project_id
        merge_template_context['params']['destination_dataset_name'] = dataset_name
        merge_template_context['params']['dataset_name_temp'] = dataset_name_temp
        merge_template_context['params']['columns'] = columns
        merge_sql = airflow_task.render_template('', merge_sql_template, merge_template_context)
        print('Merge sql:')
        print(merge_sql)
        merge_job = bigquery_client.query(merge_sql, location='US', job_config=merge_job_config)
        submit_bigquery_job(merge_job, merge_job_config)
        assert merge_job.state == 'DONE'

    # Delete temp table
    bigquery_client.delete_table(temp_table_ref)


def abi_to_event_topic(abi):
    return '0x' + event_abi_to_log_topic(abi).hex()


def abi_to_method_selector(abi):
    return '0x' + function_abi_to_4byte_selector(abi).hex()


def create_struct_string_from_schema(schema):
    def get_type(field):
        if field.get('type') == 'RECORD':
            type_str = 'STRUCT<{struct_string}>'.format(
                struct_string=create_struct_string_from_schema(field.get('fields')))
        else:
            type_str = field.get('type')

        if field.get('mode') == 'REPEATED':
            type_str = 'ARRAY<{type}>'.format(type=type_str)

        return type_str

    def get_field_def(field):
        return '`' + field.get('name') + '` ' + get_type(field)

    fields = [get_field_def(field) for field in schema]
    return ', '.join(fields)


def replace_refs(contract_address, ref_regex, project_id, dataset_name):
    return ref_regex.sub(
        r"`{project_id}.{dataset_name}.\g<1>`".format(
            project_id=project_id, dataset_name=dataset_name
        ), contract_address)


def create_dataset(client, dataset_name, project=None):
    dataset = client.dataset(dataset_name, project=project)
    try:
        logging.info('Creating new dataset ...')
        dataset = client.create_dataset(dataset)
        logging.info('New dataset created: ' + dataset_name)
    except Conflict as error:
        logging.info('Dataset already exists')

    return dataset


def get_parse_logs_sql_template(sqls_folder):
    filepath = os.path.join(sqls_folder, 'parse_logs.sql')
    with open(filepath) as file_handle:
        content = file_handle.read()
        return content


def get_parse_traces_sql_template(sqls_folder):
    filepath = os.path.join(sqls_folder, 'parse_traces.sql')
    with open(filepath) as file_handle:
        content = file_handle.read()
        return content


def get_merge_table_sql_template(sqls_folder):
    filepath = os.path.join(sqls_folder, 'merge_table.sql')
    with open(filepath) as file_handle:
        content = file_handle.read()
        return content


def get_parse_sql_template(parser_type, sqls_folder):
    return get_parse_logs_sql_template(sqls_folder) if parser_type == 'log' else get_parse_traces_sql_template(
        sqls_folder)


def read_bigquery_schema_from_dict(schema, parser_type):
    result = [
        bigquery.SchemaField(
            name='block_timestamp',
            field_type='TIMESTAMP',
            mode='REQUIRED',
            description='Timestamp of the block where this event was emitted'),
        bigquery.SchemaField(
            name='block_number',
            field_type='INTEGER',
            mode='REQUIRED',
            description='The block number where this event was emitted'),
        bigquery.SchemaField(
            name='transaction_hash',
            field_type='STRING',
            mode='REQUIRED',
            description='Hash of the transactions in which this event was emitted')
    ]
    if parser_type == 'log':
        result.append(bigquery.SchemaField(
            name='log_index',
            field_type='INTEGER',
            mode='REQUIRED',
            description='Integer of the log index position in the block of this event'))
        result.append(bigquery.SchemaField(
            name='contract_address',
            field_type='STRING',
            mode='REQUIRED',
            description='Address of the contract that produced the log'))
    elif parser_type == 'trace':
        result.append(bigquery.SchemaField(
            name='trace_address',
            field_type='STRING',
            description='Comma separated list of trace address in call tree'))
        result.append(bigquery.SchemaField(
            name='status',
            field_type='INT64',
            description='Either 1 (success) or 0 (failure, due to any operation that can cause the call itself or any top-level call to revert)'))
        result.append(bigquery.SchemaField(
            name='error',
            field_type='STRING',
            description='Error in case input parsing failed'))

    result.extend(read_bigquery_schema_from_json_recursive(schema))

    return result
