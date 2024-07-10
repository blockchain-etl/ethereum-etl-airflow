import json
import sys
import argparse
from datetime import datetime
from web3 import Web3

def validate_and_checksum_address(address):
    if address is None:
        return None
    try:
        return Web3.toChecksumAddress(address)
    except ValueError:
        print(f"Error: Invalid contract address: {address}")
        sys.exit(1)

def calculate_signature(abi):
    if isinstance(abi, str):
        abi = json.loads(abi)
    
    if abi['type'] == 'event':
        return Web3.keccak(text=f"{abi['name']}({','.join([input['type'] for input in abi['inputs']])})").hex()
    elif abi['type'] == 'function':
        return Web3.keccak(text=f"{abi['name']}({','.join([input['type'] for input in abi['inputs']])})").hex()[:10]
    else:
        raise ValueError("ABI must be for an event or function")

def generate_sql_from_json(json_file_path, date, override_contract_address=None):
    with open(json_file_path, 'r') as file:
        data = json.load(file)

    # Use the override_contract_address if provided, otherwise use the one from the JSON file
    contract_address = override_contract_address or data['parser'].get('contract_address')

    # Validate and convert contract address
    if contract_address:
        contract_address = validate_and_checksum_address(contract_address)
        # Update the contract_address in the data
        data['parser']['contract_address'] = contract_address
    else:
        print("Error: No valid contract address provided. Please use --contract_address argument.")
        sys.exit(1)

    json_string = json.dumps(data).replace("'", "''")
    signature = calculate_signature(data['parser']['abi'])
    parser_type = data['parser']['type']

    if parser_type == 'log':
        sql = generate_log_sql(json_string, signature, date)
    elif parser_type == 'trace':
        sql = generate_trace_sql(json_string, signature, date)
    else:
        raise ValueError(f"Unsupported parser type: {parser_type}")

    return sql

def generate_log_sql(json_string, signature, date):
    return f"""
WITH
abi AS 
(
SELECT
    JSON_QUERY(json_data, '$.parser.abi') AS abi,
    JSON_QUERY(json_data, '$.parser.field_mapping') AS field_mapping,
    JSON_QUERY(json_data, '$.table') AS table_details,
    JSON_EXTRACT_SCALAR(json_data, '$.parser.contract_address') AS contract_address,
    CAST(JSON_EXTRACT_SCALAR(json_data, '$.parser.type') AS STRING) AS parser_type
FROM (
    SELECT '{json_string}' AS json_data
)
),

details AS (
    SELECT 
        '{signature}' AS sig,
        abi.*
    FROM abi
),

logs AS (
  SELECT
    l.*,
    a.sig, 
    `blockchain-etl-internal.common.parse_log`(
            l.data,
            l.topics,
            REPLACE(a.abi, "'", '"')
        ) AS parsed_log
  FROM
    `bigquery-public-data.crypto_ethereum.logs` AS l
  INNER JOIN
        details AS a
    ON
        IFNULL(l.topics[SAFE_OFFSET(0)], "") = a.sig
  WHERE
    DATE(l.block_timestamp) = DATE("{date}")
    AND (a.contract_address IS NULL OR l.address = LOWER(a.contract_address))
)

SELECT * FROM logs
LIMIT 100
"""

def generate_trace_sql(json_string, signature, date):
    return f"""
WITH
abi AS 
(
SELECT
    JSON_QUERY(json_data, '$.parser.abi') AS abi,
    JSON_QUERY(json_data, '$.parser.field_mapping') AS field_mapping,
    JSON_QUERY(json_data, '$.table') AS table_details,
    JSON_EXTRACT_SCALAR(json_data, '$.parser.contract_address') AS contract_address,
    CAST(JSON_EXTRACT_SCALAR(json_data, '$.parser.type') AS STRING) AS parser_type
FROM (
    SELECT '{json_string}' AS json_data
)
),

details AS (
    SELECT 
        '{signature}' AS sig,
        abi.*
    FROM abi
),

traces AS (
  SELECT
    t.*,
    a.sig, 
    `blockchain-etl-internal.common.parse_trace`(
            t.input,
            REPLACE(a.abi, "'", '"')
        ) AS parsed_trace
  FROM
    `bigquery-public-data.crypto_ethereum.traces` AS t
  INNER JOIN
        details AS a
    ON
        STARTS_WITH(t.input, a.sig)
  WHERE
    DATE(t.block_timestamp) = DATE("{date}")
    AND (a.contract_address IS NULL OR t.to_address = LOWER(a.contract_address))
    AND t.status = 1  -- Only successful calls
    AND t.call_type = 'call'  -- Only direct calls
)

SELECT * FROM traces
LIMIT 100
"""

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate SQL from JSON file")
    parser.add_argument("json_file_path", help="Path to the JSON file")
    parser.add_argument("date", help="Date in YYYY-MM-DD format")
    parser.add_argument("--contract_address", help="Override contract address")
    
    args = parser.parse_args()

    try:
        # Validate the date format
        datetime.strptime(args.date, '%Y-%m-%d')
    except ValueError:
        print("Error: Date should be in the format YYYY-MM-DD")
        sys.exit(1)

    sql = generate_sql_from_json(args.json_file_path, args.date, args.contract_address)
    print(sql)