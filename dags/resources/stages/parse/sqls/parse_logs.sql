CREATE TEMP FUNCTION
  PARSE_LOG(data STRING, topics ARRAY<STRING>)
  RETURNS STRUCT<{{struct_fields}}>
  LANGUAGE js AS """
    var parsedEvent = {{abi}}
    return abi.decodeEvent(parsedEvent, data, topics, false);
"""
OPTIONS
  ( library="https://storage.googleapis.com/ethlab-183014.appspot.com/ethjs-abi.js" );

WITH parsed_logs AS
(SELECT
    logs.block_timestamp AS block_timestamp
    ,PARSE_LOG(logs.data, logs.topics) AS parsed
FROM {{params.source_dataset_name}}.logs AS logs
WHERE address = '{{parser.contract_address}}'
  AND topics[SAFE_OFFSET(0)] = '{{event_topic}}')
SELECT
     block_timestamp{% for column in columns %}
    ,parsed.{{ column }} AS {{ column }}{% endfor %}
FROM parsed_logs