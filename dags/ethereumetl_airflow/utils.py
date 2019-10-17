import os
from datetime import timedelta
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator


def read_file(file_path):
    with open(file_path, 'r') as f:
        return f.read()


def _build_clickhouse_http_command(parent_dir, resource, filename='-'):
    dags_folder = os.environ.get('DAGS_FOLDER', '/home/airflow/gcs/dags')
    resource_dir = 'resources/ethereumetl/'
    CLICKHOUSE_URI = Variable.get('clickhouse_uri', '')
    query_path = os.path.join(dags_folder, resource_dir, parent_dir, f'{resource}.sql')
    query = read_file(query_path)

    if filename == '-':
        return f'echo "{query}" | if curl "{CLICKHOUSE_URI}" --data-binary @{filename} 2>&1| grep -E \"Failed|Exception\"; then exit -1; fi'
    return f'eval \'if curl {CLICKHOUSE_URI}/?query={query} --data-binary @{filename} 2>&1| grep -E \"Failed|Exception\"; then exit -1; fi\''


def _build_setup_table_operator(dag, env, table_type, resource):
    parent_dir = f'schemas/{table_type}/'
    command = _build_clickhouse_http_command(parent_dir=parent_dir, resource=resource)

    task_id = f"setup_{resource}_table"
    operator = BashOperator(
        task_id=task_id,
        bash_command=command,
        execution_timeout=timedelta(hours=15),
        env=env,
        dag=dag
    )
    return operator
