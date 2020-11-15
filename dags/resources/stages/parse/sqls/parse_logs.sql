WITH parsed_logs AS
(SELECT
    logs.block_timestamp AS block_timestamp
    ,logs.block_number AS block_number
    ,logs.transaction_hash AS transaction_hash
    ,logs.log_index AS log_index
    ,logs.address AS contract_address
    ,`{{internal_project_id}}.{{dataset_name}}.{{udf_name}}`(logs.data, logs.topics) AS parsed
FROM `{{source_project_id}}.{{source_dataset_name}}.logs` AS logs
WHERE
  {% if parser.contract_address_sql %}
  address in ({{parser.contract_address_sql}})
  {% elif parser.contract_address is none %}
  true
  {% else %}
  address in ('{{parser.contract_address}}')
  {% endif %}
  AND topics[SAFE_OFFSET(0)] = '{{selector}}'
  {% if parse_all_partitions is none %}
  -- pass
  {% elif parse_all_partitions %}
  AND DATE(block_timestamp) <= '{{ds}}'
  {% else %}
  AND DATE(block_timestamp) = '{{ds}}'
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
