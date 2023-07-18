import os.path

from ethereumetl_airflow.build_verify_streaming_dag import read_file
from ethereumetl_airflow.parse.table_definition_reader import calculate_content_hash


class MockParseStateManager:
    def __init__(self, dataset_folder, updated_table_names=()):
        self.dataset_folder = dataset_folder
        self.updated_table_names = updated_table_names

    def get_content_hash(self, table_name):
        if table_name in self.updated_table_names:
            return ''

        try:
            content = read_file(os.path.join(self.dataset_folder, f'{table_name}.json'))
            return calculate_content_hash(content)
        except FileNotFoundError:
            content = read_file(os.path.join(self.dataset_folder, f'{table_name}.sql'))
            return calculate_content_hash(content)


    def set_content_hash(self, table_name, new_hash):
        pass

    def persist_state(self):
        pass