import json

import requests


def get_contract_code(address, api_token):
    response = requests.get(
        "https://api.etherscan.io/api?module=contract&action=getsourcecode&address={address}&apikey={token}".format(
            address=address, token=api_token
        ))

    response.raise_for_status()
    content = response.content
    parsed_content = json.loads(content)

    if parsed_content['status'] == '0':
        return ''
    elif parsed_content['status'] != '1':
        raise ValueError('status in response is not 0 or 1 ' + json.dumps(parsed_content))

    return content
