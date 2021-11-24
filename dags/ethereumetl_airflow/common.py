import json


def read_json_file(filepath):
    with open(filepath) as file_handle:
        content = file_handle.read()
        return json.loads(content)


def read_file(filepath):
    with open(filepath) as file_handle:
        content = file_handle.read()
        return content
