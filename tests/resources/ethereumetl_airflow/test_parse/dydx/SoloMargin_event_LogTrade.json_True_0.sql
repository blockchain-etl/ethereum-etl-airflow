CREATE OR REPLACE FUNCTION
    `blockchain-etl-internal.ethereum_dydx.parse_SoloMargin_event_LogTrade`(data STRING, topics ARRAY<STRING>)
    RETURNS STRUCT<`takerAccountOwner` STRING, `takerAccountNumber` STRING, `makerAccountOwner` STRING, `makerAccountNumber` STRING, `inputMarket` STRING, `outputMarket` STRING, `takerInputUpdate` STRUCT<`deltaWei` STRUCT<`sign` STRING, `value` STRING>, `newPar` STRUCT<`sign` STRING, `value` STRING>>, `takerOutputUpdate` STRUCT<`deltaWei` STRUCT<`sign` STRING, `value` STRING>, `newPar` STRUCT<`sign` STRING, `value` STRING>>, `makerInputUpdate` STRUCT<`deltaWei` STRUCT<`sign` STRING, `value` STRING>, `newPar` STRUCT<`sign` STRING, `value` STRING>>, `makerOutputUpdate` STRUCT<`deltaWei` STRUCT<`sign` STRING, `value` STRING>, `newPar` STRUCT<`sign` STRING, `value` STRING>>, `autoTrader` STRING>
    LANGUAGE js AS """
    var abi = {"anonymous": false, "inputs": [{"indexed": true, "name": "takerAccountOwner", "type": "address"}, {"indexed": false, "name": "takerAccountNumber", "type": "uint256"}, {"indexed": true, "name": "makerAccountOwner", "type": "address"}, {"indexed": false, "name": "makerAccountNumber", "type": "uint256"}, {"indexed": false, "name": "inputMarket", "type": "uint256"}, {"indexed": false, "name": "outputMarket", "type": "uint256"}, {"components": [{"components": [{"name": "sign", "type": "bool"}, {"name": "value", "type": "uint256"}], "name": "deltaWei", "type": "tuple"}, {"components": [{"name": "sign", "type": "bool"}, {"name": "value", "type": "uint128"}], "name": "newPar", "type": "tuple"}], "indexed": false, "name": "takerInputUpdate", "type": "tuple"}, {"components": [{"components": [{"name": "sign", "type": "bool"}, {"name": "value", "type": "uint256"}], "name": "deltaWei", "type": "tuple"}, {"components": [{"name": "sign", "type": "bool"}, {"name": "value", "type": "uint128"}], "name": "newPar", "type": "tuple"}], "indexed": false, "name": "takerOutputUpdate", "type": "tuple"}, {"components": [{"components": [{"name": "sign", "type": "bool"}, {"name": "value", "type": "uint256"}], "name": "deltaWei", "type": "tuple"}, {"components": [{"name": "sign", "type": "bool"}, {"name": "value", "type": "uint128"}], "name": "newPar", "type": "tuple"}], "indexed": false, "name": "makerInputUpdate", "type": "tuple"}, {"components": [{"components": [{"name": "sign", "type": "bool"}, {"name": "value", "type": "uint256"}], "name": "deltaWei", "type": "tuple"}, {"components": [{"name": "sign", "type": "bool"}, {"name": "value", "type": "uint128"}], "name": "newPar", "type": "tuple"}], "indexed": false, "name": "makerOutputUpdate", "type": "tuple"}, {"indexed": false, "name": "autoTrader", "type": "address"}], "name": "LogTrade", "type": "event"}

    var interface_instance = new ethers.utils.Interface([abi]);

    // A parsing error is possible for common abis that don't filter by contract address. Event signature is the same
    // for ABIs that only differ by whether a field is indexed or not. E.g. if the ABI provided has an indexed field
    // but the log entry has this field unindexed, parsing here will throw an exception.
    try {
      var parsedLog = interface_instance.parseLog({topics: topics, data: data});
    } catch (e) {
        return null;
    }

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