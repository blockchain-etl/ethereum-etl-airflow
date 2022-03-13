import pytest
import jsonschema
import json
import os

print(os.getcwd())


def load_json_file(path):
    with open(path) as f:
        obj = json.load(f)
    return obj


schema_path = 'test_cases/schema/default_schema.json'

pass_tests = ['test_cases/correct_a.json', 'test_cases/correct_b.json',
              'test_cases/correct_c.json', 'test_cases/correct_d.json']
fail_tests = ['test_cases/wrong_a.json', 'test_cases/wrong_b.json']


schema = load_json_file(schema_path)


def validate_json(filename):
    """REF: https://json-schema.org/ """
    # Describe what kind of json you expect.
    json_data = load_json_file(filename)

    try:
        jsonschema.validate(instance=json_data, schema=schema)
    except jsonschema.exceptions.ValidationError as err:
        err = f"Given JSON data is InValid for file {filename}: \n {str(err)}"
        print(err)
        return False, err

    message = "Given JSON data is Valid"
    return True, message


def get_json_files_in_dir(dir):
    all_json_files = []
    for root, dirs, files in os.walk(dir):
        for name in files:
            if name.endswith((".json")):
                all_json_files.append((root + '/' + name)[2:])
    return all_json_files


@pytest.mark.parametrize("filename", pass_tests)
def test_correct_cases(filename):
    valid, msg = validate_json(filename)
    assert (valid == True)


@pytest.mark.parametrize("filename", fail_tests)
def test_wrong_cases(filename):
    valid, msg = validate_json(filename)
    assert (valid == False)


all_files = get_json_files_in_dir('./dags/resources/stages/parse/')


@pytest.mark.parametrize("filename", all_files)
def test_file(filename):
    valid, msg = validate_json(filename)
    assert (valid == True)


@pytest.mark.parametrize("filename", all_files)
def test_dataset_name_is_correct(filename):
    json_data = load_json_file(filename)
    dataset_name = json_data.get('table', {}).get('dataset_name', None)
    assert os.path.split(filename)[0].split("/")[-1] == dataset_name

