CREATE OR REPLACE FUNCTION
    `blockchain-etl-internal.ethereum_uniswap.parse_Uniswap_event_AddLiquidity`(data STRING, topics ARRAY<STRING>)
    RETURNS STRUCT<`provider` STRING, `eth_amount` STRING, `token_amount` STRING>
    LANGUAGE js AS """
    var abi = {"anonymous": false, "inputs": [{"indexed": true, "name": "provider", "type": "address"}, {"indexed": true, "name": "eth_amount", "type": "uint256"}, {"indexed": true, "name": "token_amount", "type": "uint256"}], "name": "AddLiquidity", "type": "event"}

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