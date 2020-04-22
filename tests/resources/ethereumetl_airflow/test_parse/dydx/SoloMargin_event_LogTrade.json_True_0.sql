CREATE OR REPLACE FUNCTION
    `blockchain-etl.ethereum_dydx_internal.parse_SoloMargin_event_LogTrade`(data STRING, topics ARRAY<STRING>)
    RETURNS STRUCT<>
    LANGUAGE js AS """
    var abi = {"anonymous": false, "inputs": [{"indexed": true, "name": "takerAccountOwner", "type": "address"}, {"indexed": false, "name": "takerAccountNumber", "type": "uint256"}, {"indexed": true, "name": "makerAccountOwner", "type": "address"}, {"indexed": false, "name": "makerAccountNumber", "type": "uint256"}, {"indexed": false, "name": "inputMarket", "type": "uint256"}, {"indexed": false, "name": "outputMarket", "type": "uint256"}, {"components": [{"components": [{"name": "sign", "type": "bool"}, {"name": "value", "type": "uint256"}], "name": "deltaWei", "type": "tuple"}, {"components": [{"name": "sign", "type": "bool"}, {"name": "value", "type": "uint128"}], "name": "newPar", "type": "tuple"}], "indexed": false, "name": "takerInputUpdate", "type": "tuple"}, {"components": [{"components": [{"name": "sign", "type": "bool"}, {"name": "value", "type": "uint256"}], "name": "deltaWei", "type": "tuple"}, {"components": [{"name": "sign", "type": "bool"}, {"name": "value", "type": "uint128"}], "name": "newPar", "type": "tuple"}], "indexed": false, "name": "takerOutputUpdate", "type": "tuple"}, {"components": [{"components": [{"name": "sign", "type": "bool"}, {"name": "value", "type": "uint256"}], "name": "deltaWei", "type": "tuple"}, {"components": [{"name": "sign", "type": "bool"}, {"name": "value", "type": "uint128"}], "name": "newPar", "type": "tuple"}], "indexed": false, "name": "makerInputUpdate", "type": "tuple"}, {"components": [{"components": [{"name": "sign", "type": "bool"}, {"name": "value", "type": "uint256"}], "name": "deltaWei", "type": "tuple"}, {"components": [{"name": "sign", "type": "bool"}, {"name": "value", "type": "uint128"}], "name": "newPar", "type": "tuple"}], "indexed": false, "name": "makerOutputUpdate", "type": "tuple"}, {"indexed": false, "name": "autoTrader", "type": "address"}], "name": "LogTrade", "type": "event"}

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