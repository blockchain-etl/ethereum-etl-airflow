CREATE TEMP FUNCTION
  PARSE_TRACE(data STRING)
  RETURNS STRUCT<{{params.struct_fields}}>
  LANGUAGE js AS """
    function getKeys(params, key) {
        var result = [];
        
        for (var i = 0; i < params.length; i++) { 
            var value = params[i][key];
            if (!value) {
                value = '';
            }
            result.push(value);
        }
    
        return result;
    }

    function decodeInput(method, data) {
        const outputNames = getKeys(method.inputs, 'name', true);
        const outputTypes = getKeys(method.inputs, 'type');
    
        return abi.decodeParams(outputNames, outputTypes, data, false);
    }

    var methodAbi = {{params.abi}};
    return decodeInput(methodAbi, data);
"""
OPTIONS
  ( library="https://storage.googleapis.com/ethlab-183014.appspot.com/ethjs-abi.js" );

WITH parsed_traces AS
(SELECT
    traces.block_timestamp AS block_timestamp
    ,traces.block_number AS block_number
    ,traces.transaction_hash AS transaction_hash
    ,traces.trace_address AS trace_address
    ,PARSE_TRACE(traces.input) AS parsed
FROM `{{params.source_project_id}}.{{params.source_dataset_name}}.traces` AS traces
WHERE to_address = '{{params.parser.contract_address}}'
  AND STARTS_WITH(traces.input, '{{params.method_selector}}')
  {% if not params.parse_all_partitions %}
  AND DATE(block_timestamp) = '{{ds}}'
  {% endif %}
  )
SELECT
     block_timestamp
     ,block_number
     ,transaction_hash
     ,trace_address{% for column in params.columns %}
    ,parsed.{{ column }} AS `{{ column }}`{% endfor %}
FROM parsed_traces
WHERE true
    {% if not params.parse_all_partitions %}
    AND DATE(parsed_traces.block_timestamp) = '{{ds}}'
    {% endif %}