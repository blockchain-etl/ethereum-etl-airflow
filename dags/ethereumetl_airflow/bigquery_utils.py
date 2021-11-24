import json
import logging

from google.cloud import bigquery
from google.api_core.exceptions import Conflict, NotFound

from ethereumetl_airflow.common import read_file


def submit_bigquery_job(job, configuration):
    try:
        logging.info('Creating a job: ' + json.dumps(configuration.to_api_repr()))
        result = job.result()
        logging.info(result)
        assert job.errors is None or len(job.errors) == 0
        return result
    except Exception:
        logging.info(job.errors)
        raise


def read_bigquery_schema_from_file(filepath):
    result = []
    file_content = read_file(filepath)
    json_content = json.loads(file_content)
    return read_bigquery_schema_from_json_recursive(json_content)


def read_bigquery_schema_from_json_recursive(json_schema):
    """
    CAUTION: Recursive function
    This method can generate BQ schemas for nested records
    """
    result = []
    for field in json_schema:
        if field.get('type').lower() == 'record' and field.get('fields'):
            schema = bigquery.SchemaField(
                name=field.get('name'),
                field_type=field.get('type', 'STRING'),
                mode=field.get('mode', 'NULLABLE'),
                description=field.get('description'),
                fields=read_bigquery_schema_from_json_recursive(field.get('fields'))
            )
        else:
            schema = bigquery.SchemaField(
                name=field.get('name'),
                field_type=field.get('type', 'STRING'),
                mode=field.get('mode', 'NULLABLE'),
                description=field.get('description')
            )
        result.append(schema)
    return result


def query(bigquery_client, sql, destination=None, priority=bigquery.QueryPriority.INTERACTIVE):
    job_config = bigquery.QueryJobConfig()
    job_config.destination = destination
    job_config.priority = priority
    logging.info('Executing query: ' + sql)
    query_job = bigquery_client.query(sql, location='US', job_config=job_config)
    submit_bigquery_job(query_job, job_config)
    assert query_job.state == 'DONE'


def create_view(bigquery_client, sql, table_ref, description=None):
    table = bigquery.Table(table_ref)
    table.view_query = sql
    if description is not None:
        table.description = description

    logging.info('Creating view: ' + json.dumps(table.to_api_repr()))

    try:
        table = bigquery_client.create_table(table)
    except Conflict:
        # https://cloud.google.com/bigquery/docs/managing-views
        table = bigquery_client.update_table(table, ['view_query'])
    assert table.table_id == table_ref.table_id


def does_table_exist(bigquery_client, table_ref):
    try:
        table = bigquery_client.get_table(table_ref)
    except NotFound:
        return False
    return True


def share_dataset_all_users_read(bigquery_client, full_dataset_name):
    bigquery.AccessEntry.ENTITY_TYPES = ["userByEmail", "groupByEmail", "domain", "specialGroup", "view", "iamMember"]

    role = 'READER'
    entity_type = 'iamMember'
    entity_id = 'allUsers'

    dataset = bigquery_client.get_dataset(full_dataset_name)
    entries = list(dataset.access_entries)
    is_shared = False
    for entry in entries:
        if entry.role == role and entry.entity_type == entity_type and entry.entity_id == entity_id:
            is_shared = True

    if not is_shared:
        entry = bigquery.AccessEntry(
            role=role,
            entity_type=entity_type,
            entity_id=entity_id,
        )
        entries.append(entry)
        dataset.access_entries = entries
        dataset = bigquery_client.update_dataset(dataset, ["access_entries"])
        logging.info('Updated dataset permissions')
    else:
        logging.info('The dataset is already shared')