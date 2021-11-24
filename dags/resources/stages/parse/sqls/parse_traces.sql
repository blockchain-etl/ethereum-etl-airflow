WITH parsed_traces AS
(SELECT
    traces.block_timestamp AS block_timestamp
    ,traces.block_number AS block_number
    ,traces.transaction_hash AS transaction_hash
    ,traces.transaction_index AS transaction_index
    ,traces.trace_address AS trace_address
    ,traces.to_address AS to_address
    ,traces.status AS status
    ,`{{internal_project_id}}.{{dataset_name}}.{{udf_name}}`(traces.input) AS parsed
FROM `{{full_source_table_name}}` AS traces
WHERE to_address IN (
    {% if parser.contract_address_sql %}
    {{parser.contract_address_sql}}
    {% else %}
    lower('{{parser.contract_address}}')
    {% endif %}
  )
  AND STARTS_WITH(traces.input, '{{selector}}')

  {% if parse_mode == 'live' %}
  -- live
  {% elif parse_mode == 'history_all_dates' %}
  AND DATE(block_timestamp) <= '{{ds}}'
  {% elif parse_mode == 'history_single_date' %}
  AND DATE(block_timestamp) = '{{ds}}'
  AND _input_partition_index = MOD(ABS(FARM_FINGERPRINT('{{selector}}')), 3999)
  {% else %}
  UNCOMPILABLE SQL: unknown parse_mode {{parse_mode}}
  {% endif %}

  )
SELECT
     block_timestamp
     ,block_number
     ,transaction_hash
     ,transaction_index
     ,trace_address
     ,to_address
     ,status
     ,parsed.error AS error
     {% for column in table.schema %}
    ,parsed.{{ column.name }} AS `{{ column.name }}`
    {% endfor %}
FROM parsed_traces
