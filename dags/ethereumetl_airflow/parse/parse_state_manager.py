import json
import logging
import time
from datetime import datetime, timedelta

from google.cloud import storage
from google.cloud.exceptions import NotFound

FALLBACK_DATASET = "common"


class ParseStateManager:
    def __init__(self, dataset_name, state_bucket, bucket_path, project=None):
        self.dataset_name = dataset_name
        self.state_bucket = state_bucket
        self.bucket_path = bucket_path

        if project is not None:
            self.storage_client = storage.Client(project=project)
        else:
            self.storage_client = storage.Client()

        self.sync_state_file()

    def sync_state_file(self):
        state_file = self._download_state_file(self.dataset_name)
        self.is_state_empty = False
        if state_file:
            self.state = json.loads(state_file)
            self.is_state_empty = False
        else:
            self.state = {}
            self.is_state_empty = True

    def get_content_hash(self, table_name):
        content_hash = self.state.get(table_name)
        if not content_hash:
            content_hash = ""
        return content_hash

    def set_content_hash(self, table_name, new_hash):
        self.state[table_name] = new_hash

    def get_last_ds(self):
        last_ds = self.state.get("_last_ds")
        if last_ds is None:
            raise ValueError("_last_ds is None in parse state")
        return last_ds

    def get_fallback_last_ds(self):
        state_file = self._download_state_file(FALLBACK_DATASET)
        if state_file:
            state = json.loads(state_file)
            last_ds = state.get("_last_ds")
            if last_ds is None:
                raise ValueError("_last_ds is None in parse state")
            return last_ds
        else:
            raise ValueError(f"Fallback dataset {FALLBACK_DATASET} is not available")

    def set_last_ds(self, ds):
        self.state["_last_ds"] = ds

    def persist_state(self):
        self._check_version_and_last_ds()
        self._set_new_version()

        bucket = self.storage_client.get_bucket(self.state_bucket)
        blob = bucket.blob(self._build_state_file_name(self.dataset_name))
        state_str = json.dumps(self.state)
        logging.info(f"Persisting parse state: {state_str}")
        blob.upload_from_string(state_str)

    def _build_state_file_name(self, dataset_name):
        return f"{self.bucket_path}/{dataset_name}/state.json"

    def _download_state_file(self, dataset_name):
        bucket = self.storage_client.get_bucket(self.state_bucket)

        blob = bucket.blob(self._build_state_file_name(dataset_name))

        try:
            content = blob.download_as_text()
            return content
        except NotFound:
            return None

    def _check_version_and_last_ds(self):
        # Optimistic locking to prevent race conditions with CI/CD
        state_file = self._download_state_file(self.dataset_name)
        if not state_file:
            return
        else:
            state = json.loads(state_file)

        previous_version = state.get("_version")
        if not previous_version:
            return

        current_version = self.state.get("_version")
        if previous_version != current_version:
            raise ValueError(
                f"The state version on the bucket is different from the version in memory,"
                f"bucket: {previous_version}, memory: {current_version}. "
                f"Another process must have run in parallel"
            )

        previous_last_ds = state.get("_last_ds")
        if not previous_last_ds:
            return

        current_last_ds = self.state.get("_last_ds")
        if not self._check_last_ds(previous_last_ds, current_last_ds):
            raise ValueError(
                f"Some days may have been skipped. Current _last_ds: {current_last_ds}, previous _last_ds: {previous_last_ds}"
            )

    def _check_last_ds(self, previous_last_ds, current_last_ds):
        previous_date = datetime.strptime(previous_last_ds, "%Y-%m-%d")
        current_date = datetime.strptime(current_last_ds, "%Y-%m-%d")

        if current_date != previous_date and current_date != previous_date + timedelta(
            days=1
        ):
            return False
        else:
            return True

    def _set_new_version(self):
        new_version = int(
            time.time() * 1000
        )  # time.time() returns seconds, so we multiply by 1000 for milliseconds
        self.state["_version"] = str(new_version)
