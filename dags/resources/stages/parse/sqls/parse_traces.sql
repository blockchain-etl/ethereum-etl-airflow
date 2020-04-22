CREATE TEMP FUNCTION
    PARSE_TRACE(data STRING)
    RETURNS STRUCT<{{struct_fields}}, error STRING>
    LANGUAGE js AS """
    var abi = {{abi}};
    var interface_instance = new ethers.utils.Interface([abi]);

    var result = {};
    try {
        var parsedTransaction = interface_instance.parseTransaction({data: data});
        var parsedArgs = parsedTransaction.args;

        if (parsedArgs && parsedArgs.length >= abi.inputs.length) {
            for (var i = 0; i < abi.inputs.length; i++) {
                var paramName = abi.inputs[i].name;
                var paramValue = parsedArgs[i];
                if (abi.inputs[i].type === 'address' && typeof paramValue === 'string') {
                    // For consistency all addresses are lowercase.
                    paramValue = paramValue.toLowerCase();
                }
                result[paramName] = paramValue;
            }
        } else {
            result['error'] = 'Parsed transaction args is empty or has too few values.';
        }
    } catch (e) {
        result['error'] = e.message;
    }

    return result;
"""
OPTIONS
  ( library="gs://blockchain-etl-bigquery/ethers.js" );

WITH parsed_traces AS
(SELECT
    traces.block_timestamp AS block_timestamp
    ,traces.block_number AS block_number
    ,traces.transaction_hash AS transaction_hash
    ,traces.trace_address AS trace_address
    ,traces.status AS status
    ,PARSE_TRACE(traces.input) AS parsed
FROM `{{source_project_id}}.{{source_dataset_name}}.traces` AS traces
WHERE to_address = '{{parser.contract_address}}'
  AND STARTS_WITH(traces.input, '{{method_selector}}')
  {% if parse_all_partitions %}
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
