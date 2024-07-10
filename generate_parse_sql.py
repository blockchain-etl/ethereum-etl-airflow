import json
import sys
from datetime import datetime
from web3 import Web3

def calculate_signature(abi):
    if isinstance(abi, str):
        abi = json.loads(abi)
    
    if abi['type'] == 'event':
        return Web3.keccak(text=f"{abi['name']}({','.join([input['type'] for input in abi['inputs']])})").hex()
    elif abi['type'] == 'function':
        return Web3.keccak(text=f"{abi['name']}({','.join([input['type'] for input in abi['inputs']])})").hex()[:10]
    else:
        raise ValueError("ABI must be for an event or function")

def generate_sql_from_json(json_file_path, date):
    with open(json_file_path, 'r') as file:
        data = json.load(file)

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
    if len(sys.argv) != 3:
        print("Usage: python generate_parse_sql.py <path_to_json_file> <date>")
        sys.exit(1)

    json_file_path = sys.argv[1]
    date_str = sys.argv[2]

    try:
        # Validate the date format
        datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        print("Error: Date should be in the format YYYY-MM-DD")
        sys.exit(1)

    sql = generate_sql_from_json(json_file_path, date_str)
    print(sql)