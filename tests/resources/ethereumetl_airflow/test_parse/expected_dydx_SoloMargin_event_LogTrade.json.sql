CREATE TEMP FUNCTION
    PARSE_LOG(data STRING, topics ARRAY<STRING>)
    RETURNS STRUCT<`takerAccountOwner` STRING, `takerAccountNumber` STRING, `makerAccountOwner` STRING, `makerAccountNumber` STRING, `inputMarket` STRING, `outputMarket` STRING, `takerInputUpdate` STRUCT<`deltaWei` STRUCT<`sign` STRING, `value` STRING>, `newPar` STRUCT<`sign` STRING, `value` STRING>>, `takerOutputUpdate` STRUCT<`deltaWei` STRUCT<`sign` STRING, `value` STRING>, `newPar` STRUCT<`sign` STRING, `value` STRING>>, `makerInputUpdate` STRUCT<`deltaWei` STRUCT<`sign` STRING, `value` STRING>, `newPar` STRUCT<`sign` STRING, `value` STRING>>, `makerOutputUpdate` STRUCT<`deltaWei` STRUCT<`sign` STRING, `value` STRING>, `newPar` STRUCT<`sign` STRING, `value` STRING>>, `autoTrader` STRING>
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

WITH parsed_logs AS
(SELECT
    logs.block_timestamp AS block_timestamp
    ,logs.block_number AS block_number
    ,logs.transaction_hash AS transaction_hash
    ,logs.log_index AS log_index
    ,logs.address AS contract_address
    ,PARSE_LOG(logs.data, logs.topics) AS parsed
FROM `bigquery-public-data.crypto_ethereum.logs` AS logs
WHERE address in (

    '0x1e0447b19bb6ecfdae1e4ae1694b0c3659614e4e'

  )
  AND topics[SAFE_OFFSET(0)] = '0x54d4cc60cf7d570631cc1a58942812cb0fc461713613400f56932040c3497d19'

  AND DATE(block_timestamp) <= '2020-01-01'

  )
SELECT
     block_timestamp
     ,block_number
     ,transaction_hash
     ,log_index
     ,contract_address

    ,parsed.takerAccountOwner AS `takerAccountOwner`
    ,parsed.takerAccountNumber AS `takerAccountNumber`
    ,parsed.makerAccountOwner AS `makerAccountOwner`
    ,parsed.makerAccountNumber AS `makerAccountNumber`
    ,parsed.inputMarket AS `inputMarket`
    ,parsed.outputMarket AS `outputMarket`
    ,parsed.takerInputUpdate AS `takerInputUpdate`
    ,parsed.takerOutputUpdate AS `takerOutputUpdate`
    ,parsed.makerInputUpdate AS `makerInputUpdate`
    ,parsed.makerOutputUpdate AS `makerOutputUpdate`
    ,parsed.autoTrader AS `autoTrader`
FROM parsed_logs