CREATE OR REPLACE FUNCTION
    `{{destination_project_id}}.{{destination_dataset_name}}.{{udf_name}}`(data STRING, topics ARRAY<STRING>)
    RETURNS STRUCT<{{struct_fields}}>
    LANGUAGE js AS """
    var abi = {{abi}}

    var interface_instance = new ethers.utils.Interface([abi]);

    var parsedLog = interface_instance.parseLog({topics: topics, data: data});

    var parsedValues = parsedLog.values;

    var transformParams = function(params, abiInputs) {
        var result = {};
        if (params && params.length >= abiInputs.length) {
            for (var i = 0; i < abiInputs.length; i++) {
                var paramName = abiInputs[i].name;
                var paramValue = params[i];
                if (abiInputs[i].type === 'address' && typeof paramValue === 'string') {
                    // For consistency all addresses are lower-cased.
                    paramValue = paramValue.toLowerCase();
                }
                if (ethers.utils.Interface.isIndexed(paramValue)) {
                    paramValue = paramValue.hash;
                }
                if (abiInputs[i].type === 'tuple' && 'components' in abiInputs[i]) {
                    paramValue = transformParams(paramValue, abiInputs[i].components)
                }
                result[paramName] = paramValue;
            }
        }
        return result;
    };

    var result = transformParams(parsedValues, abi.inputs);

    return result;
"""
OPTIONS
  ( library="gs://blockchain-etl-bigquery/ethers.js" );