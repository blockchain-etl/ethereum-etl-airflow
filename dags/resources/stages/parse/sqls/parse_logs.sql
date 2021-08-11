WITH parsed_logs AS
(SELECT
    logs.block_timestamp AS block_timestamp
    ,logs.block_number AS block_number
    ,logs.transaction_hash AS transaction_hash
    ,logs.log_index AS log_index
    ,logs.address AS contract_address
    ,`{{internal_project_id}}.{{dataset_name}}.{{udf_name}}`(logs.data, logs.topics) AS parsed
FROM `{{full_source_table_name}}` AS logs
WHERE
  {% if parser.contract_address_sql %}
  address in ({{parser.contract_address_sql}})
  {% elif parser.contract_address is none %}
  true
  {% else %}
  address in (lower('{{parser.contract_address}}'))
  {% endif %}
  AND topics[SAFE_OFFSET(0)] = '{{selector}}'

  {% if parse_mode == 'live' %}
  -- live
  {% elif parse_mode == 'history_all_dates' %}
  AND DATE(block_timestamp) <= '{{ds}}'
  {% elif parse_mode == 'history_single_date' %}
  AND DATE(block_timestamp) = '{{ds}}'
  AND _topic_partition_index = MOD(ABS(FARM_FINGERPRINT('{{selector}}')), 3999)
  {% else %}
  UNCOMPILABLE SQL: unknown parse_mode {{parse_mode}}
  {% endif %}

  )
SELECT
     block_timestamp
     ,block_number
     ,transaction_hash
     ,log_index
     ,contract_address
     {% for column in table.schema %}
    ,parsed.{{ column.name }} AS `{{ column.name }}`{% endfor %}
FROM parsed_logs
WHERE parsed IS NOT NULL
