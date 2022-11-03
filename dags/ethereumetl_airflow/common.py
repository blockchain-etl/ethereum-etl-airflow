import json
import logging
import os
from glob import glob


def read_json_file(filepath):
    with open(filepath) as file_handle:
        content = file_handle.read()
        return json.loads(content)


def read_file(filepath):
    with open(filepath) as file_handle:
        content = file_handle.read()
        return content


def get_list_of_files(dataset_folder, filter='*.json'):
    logging.info('get_list_of_files')
    logging.info(dataset_folder)
    logging.info(os.path.join(dataset_folder, filter))
    return [f for f in glob(os.path.join(dataset_folder, filter))]