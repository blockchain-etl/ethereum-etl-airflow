import json
import sys
from datetime import datetime

def generate_sql_from_json(json_file_path, date):
    with open(json_file_path, 'r') as file:
        data = json.load(file)

    json_string = json.dumps(data).replace("'", "''")

    sql = f"""
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
        `nansen-datasets-dev.z_arax.calculateSighash`(REPLACE(abi.abi, "'", '"')) AS sig,
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

    return sql

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