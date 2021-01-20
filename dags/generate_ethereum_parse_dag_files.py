from glob import glob

if __name__ == "__main__":
    table_definitions_folder = 'resources/stages/parse/table_definitions/*'

    for folder in glob(table_definitions_folder):
        dataset = folder.split('/')[-1]

        with open(f'ethereum_parse_{dataset}_dag.py', 'w') as f:
            f.write(f'''
from ethereum_parse_dag import build_parse_dag_from_dataset_name

# airflow DAG
dag = build_parse_dag_from_dataset_name('{dataset}')
''')
