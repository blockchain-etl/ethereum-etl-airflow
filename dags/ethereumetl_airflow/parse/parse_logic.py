import json
import logging
import re
import time

from eth_utils import event_abi_to_log_topic, function_abi_to_4byte_selector
from google.cloud import bigquery

from google.api_core.exceptions import Conflict

from ethereumetl_airflow.bigquery_utils import submit_bigquery_job, read_bigquery_schema_from_json_recursive, query, \
    create_view
from ethereumetl_airflow.parse.templates import render_parse_udf_template, render_parse_sql_template, \
    render_merge_template, render_stitch_view_template

ref_regex = re.compile(r"ref\(\'([^']+)\'\)")


def parse(
        bigquery_client,
        table_definition,
        ds,
        source_project_id,
        source_dataset_name,
        destination_project_id,
        sqls_folder,
        parse_all_partitions,
        time_func=time.time
):

    internal_project_id = destination_project_id + '-internal'

    create_or_replace_internal_view(
        bigquery_client=bigquery_client,
        table_definition=table_definition,
        ds=ds,
        source_project_id=source_project_id,
        source_dataset_name=source_dataset_name,
        internal_project_id=internal_project_id,
        destination_project_id=destination_project_id,
        sqls_folder=sqls_folder,
        parse_all_partitions=parse_all_partitions
    )

    create_or_update_history_table(
        bigquery_client=bigquery_client,
        table_definition=table_definition,
        ds=ds,
        source_project_id=source_project_id,
        source_dataset_name=source_dataset_name,
        internal_project_id=internal_project_id,
        destination_project_id=destination_project_id,
        sqls_folder=sqls_folder,
        parse_all_partitions=parse_all_partitions,
        time_func=time_func
    )

    create_or_replace_stitch_view(
        bigquery_client=bigquery_client,
        table_definition=table_definition,
        ds=ds,
        internal_project_id=internal_project_id,
        destination_project_id=destination_project_id,
        sqls_folder=sqls_folder,
    )


def create_or_replace_internal_view(
        bigquery_client,
        table_definition,
        ds,
        source_project_id,
        source_dataset_name,
        internal_project_id,
        destination_project_id,
        sqls_folder,
        parse_all_partitions
):
    dataset_name = 'ethereum_' + table_definition['table']['dataset_name']
    table_name = table_definition['table']['table_name']

    parser_type = table_definition['parser'].get('type', 'log')

    udf_name = 'parse_{}'.format(table_name)

    # # # Create UDF

    sql = render_parse_udf_template(
        sqls_folder,
        parser_type,
        internal_project_id=internal_project_id,
        dataset_name=dataset_name,
        udf_name=udf_name,
        abi=json.dumps(table_definition['parser']['abi']),
        struct_fields=create_struct_string_from_schema(table_definition['table']['schema'])
    )
    query(bigquery_client, sql)

    # # # Create view

    sql = generate_parse_sql_template(
        sqls_folder,
        parser_type,
        source_project_id=source_project_id,
        source_dataset_name=source_dataset_name,
        internal_project_id=internal_project_id,
        destination_project_id=destination_project_id,
        dataset_name=dataset_name,
        udf_name=udf_name,
        table_definition=table_definition,
        parse_all_partitions=parse_all_partitions,
        ds=ds
    )

    dataset = create_dataset(bigquery_client, dataset_name, internal_project_id)
    dest_view_ref = dataset.table(table_name)

    create_view(bigquery_client, sql, dest_view_ref)


def create_or_update_history_table(
        bigquery_client,
        table_definition,
        ds,
        source_project_id,
        source_dataset_name,
        internal_project_id,
        destination_project_id,
        sqls_folder,
        parse_all_partitions,
        time_func=time.time
):
    dataset_name = 'ethereum_' + table_definition['table']['dataset_name']
    table_name = table_definition['table']['table_name']
    history_table_name = table_name + '_history'

    schema = table_definition['table']['schema']
    parser_type = table_definition['parser'].get('type', 'log')

    schema = read_bigquery_schema_from_dict(schema, parser_type)

    # # # Create a temporary table

    dataset_name_temp = 'parse_temp'
    create_dataset(bigquery_client, dataset_name_temp)
    temp_table_name = 'temp_{table_name}_{milliseconds}' \
        .format(table_name=table_name, milliseconds=int(round(time_func() * 1000)))
    temp_table_ref = bigquery_client.dataset(dataset_name_temp).table(temp_table_name)

    temp_table = bigquery.Table(temp_table_ref, schema=schema)

    table_description = table_definition['table']['table_description']
    temp_table.description = table_description
    temp_table.time_partitioning = bigquery.TimePartitioning(field='block_timestamp')
    logging.info('Creating table: ' + json.dumps(temp_table.to_api_repr()))
    temp_table = bigquery_client.create_table(temp_table)
    assert temp_table.table_id == temp_table_name

    # # # Query to temporary table

    udf_name = 'parse_{}'.format(table_name)
    sql = generate_parse_sql_template(
        sqls_folder,
        parser_type,
        source_project_id=source_project_id,
        source_dataset_name=source_dataset_name,
        internal_project_id=internal_project_id,
        destination_project_id=destination_project_id,
        dataset_name=dataset_name,
        udf_name=udf_name,
        table_definition=table_definition,
        parse_all_partitions=parse_all_partitions,
        ds=ds
    )
    query(bigquery_client, sql, destination=temp_table_ref)

    # # # Copy / merge to destination

    if parse_all_partitions:
        # Copy temporary table to destination
        copy_job_config = bigquery.CopyJobConfig()
        copy_job_config.write_disposition = 'WRITE_TRUNCATE'
        dataset = create_dataset(bigquery_client, dataset_name, internal_project_id)
        dest_table_ref = dataset.table(history_table_name)
        copy_job = bigquery_client.copy_table(temp_table_ref, dest_table_ref, location='US', job_config=copy_job_config)
        submit_bigquery_job(copy_job, copy_job_config)
        assert copy_job.state == 'DONE'
        # Need to do update description as copy above won't respect the description in case destination table
        # already exists
        table = bigquery_client.get_table(dest_table_ref)
        table.description = table_description
        table = bigquery_client.update_table(table, ["description"])
        assert table.description == table_description
    else:
        merge_sql = render_merge_template(
            sqls_folder,
            table_schema=schema,
            internal_project_id=internal_project_id,
            dataset_name=dataset_name,
            destination_table_name=history_table_name,
            dataset_name_temp=dataset_name_temp,
            source_table=temp_table_name,
            ds=ds
        )
        query(bigquery_client, merge_sql)

    # Delete temp table
    bigquery_client.delete_table(temp_table_ref)


def create_or_replace_stitch_view(
        bigquery_client,
        table_definition,
        ds,
        destination_project_id,
        internal_project_id,
        sqls_folder
):
    dataset_name = 'ethereum_' + table_definition['table']['dataset_name']
    table_name = table_definition['table']['table_name']
    history_table_name = table_name + '_history'

    # # # Create view

    sql = render_stitch_view_template(
        sqls_folder=sqls_folder,
        internal_project_id=internal_project_id,
        dataset_name=dataset_name,
        table_name=table_name,
        history_table_name=history_table_name,
        ds=ds
    )

    print('Stitch view: ' + sql)

    dataset = create_dataset(bigquery_client, dataset_name, destination_project_id)
    dest_view_ref = dataset.table(table_name)

    create_view(bigquery_client, sql, dest_view_ref)


def generate_parse_sql_template(
        sqls_folder,
        parser_type,
        source_project_id,
        source_dataset_name,
        internal_project_id,
        destination_project_id,
        dataset_name,
        udf_name,
        table_definition,
        parse_all_partitions,
        ds):
    contract_address = table_definition['parser']['contract_address']
    if not contract_address.startswith('0x'):
        table_definition['parser']['contract_address_sql'] = replace_refs(
            contract_address, ref_regex, destination_project_id, dataset_name
        )

    selector = abi_to_selector(parser_type, table_definition['parser']['abi'])

    sql = render_parse_sql_template(
        sqls_folder,
        parser_type,
        source_project_id=source_project_id,
        source_dataset_name=source_dataset_name,
        internal_project_id=internal_project_id,
        dataset_name=dataset_name,
        udf_name=udf_name,
        parser=table_definition['parser'],
        table=table_definition['table'],
        selector=selector,
        parse_all_partitions=parse_all_partitions,
        ds=ds
    )
    return sql


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


def abi_to_selector(parser_type, abi):
    if parser_type == 'log':
        return '0x' + event_abi_to_log_topic(abi).hex()
    else:
        return '0x' + function_abi_to_4byte_selector(abi).hex()

