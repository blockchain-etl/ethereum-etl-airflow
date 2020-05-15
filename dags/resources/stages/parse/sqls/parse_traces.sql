WITH parsed_traces AS
(SELECT
    traces.block_timestamp AS block_timestamp
    ,traces.block_number AS block_number
    ,traces.transaction_hash AS transaction_hash
    ,traces.trace_address AS trace_address
    ,traces.status AS status
    ,`{{internal_project_id}}.{{dataset_name}}.{{udf_name}}`(traces.input) AS parsed
FROM `{{source_project_id}}.{{source_dataset_name}}.traces` AS traces
WHERE to_address = '{{parser.contract_address}}'
  AND STARTS_WITH(traces.input, '{{selector}}')
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
     ,trace_address
     ,status
     ,parsed.error AS error
     {% for column in table.schema %}
    ,parsed.{{ column.name }} AS `{{ column.name }}`
    {% endfor %}
FROM parsed_traces
