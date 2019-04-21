import json
from random import shuffle

import requests


def get_contract_abi(address):
    all_tokens = ['token', 'token2']
    shuffle(all_tokens)
    response = requests.get(
        "https://api.etherscan.io/api?module=contract&action=getabi&address={address}&apikey={token}".format(
            address=address, token=all_tokens[0]
        ))

    response.raise_for_status()

    content = response.content

    parsed_content = json.loads(content)

    if parsed_content['status'] == '0':
        return ''
    elif parsed_content['status'] != '1':
        raise ValueError('status in response is not 0 or 1 ' + json.dumps(parsed_content))

    return parsed_content['result']
