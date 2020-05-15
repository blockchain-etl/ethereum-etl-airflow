from google.cloud.bigquery import DatasetReference, Dataset


PROJECT = 'my-project'

class MockBigqueryClient:
    def __init__(self):
        self.queries = []

    def dataset(self, dataset_id, project=None):
        if project is None:
            project = PROJECT
        return DatasetReference(project, dataset_id)

    def create_dataset(self, dataset_ref):
        return Dataset(dataset_ref)

    def create_table(self, table_ref):
        if table_ref.view_query is not None:
            self.queries.append(table_ref.view_query)
        return table_ref

    def copy_table(self, *args, **kwargs):
        return MockBigqueryJob()

    def update_table(self, table_ref, *args, **kwargs):
        return table_ref

    def delete_table(self, table_ref, *args, **kwargs):
        return table_ref

    def get_table(self, table_ref):
        return table_ref

    def query(self, sql, *args, **kwargs):
        self.queries.append(sql)
        return MockBigqueryJob()


class MockBigqueryJob:
    state = 'DONE'
    errors = None
    description = None

    def result(self):
        return 'success'
