import json
import logging

from google.cloud.exceptions import NotFound
from google.cloud import storage


class ParseStateManager:
    def __init__(self, dataset_name, state_bucket, bucket_path):
        self.dataset_name = dataset_name
        self.state_bucket = state_bucket
        self.bucket_path = bucket_path

        self.storage_client = storage.Client()

        state_file = self._download_state_file()
        if state_file:
            self.state = json.loads(state_file)
        else:
            self.state = {}

    def get_content_hash(self, table_name):
        content_hash = self.state.get(table_name)
        if not content_hash:
            content_hash = ''
        return content_hash

    def set_content_hash(self, table_name, new_hash):
        self.state[table_name] = new_hash

    def persist_state(self):
        bucket = self.storage_client.get_bucket(self.state_bucket)
        blob = bucket.blob(self._build_state_file_name())
        state_str = json.dumps(self.state)
        logging.info(f'Persisting parse state: {state_str}')
        blob.upload_from_string(state_str)

    def _build_state_file_name(self):
        return f'{self.bucket_path}/{self.dataset_name}/state.json'

    def _download_state_file(self):
        bucket = self.storage_client.get_bucket(self.state_bucket)

        blob = bucket.blob(self._build_state_file_name())

        try:
            content = blob.download_as_text()
            return content
        except NotFound:
            return None
