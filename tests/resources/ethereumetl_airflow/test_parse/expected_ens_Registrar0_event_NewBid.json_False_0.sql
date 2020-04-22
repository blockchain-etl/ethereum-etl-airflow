CREATE TEMP FUNCTION
    PARSE_LOG(data STRING, topics ARRAY<STRING>)
    RETURNS STRUCT<`hash` STRING, `bidder` STRING, `deposit` STRING>
    LANGUAGE js AS """
    var abi = {"anonymous": false, "inputs": [{"indexed": true, "name": "hash", "type": "bytes32"}, {"indexed": true, "name": "bidder", "type": "address"}, {"indexed": false, "name": "deposit", "type": "uint256"}], "name": "NewBid", "type": "event"}

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

    '0x6090a6e47849629b7245dfa1ca21d94cd15878ef'

  )
  AND topics[SAFE_OFFSET(0)] = '0xb556ff269c1b6714f432c36431e2041d28436a73b6c3f19c021827bbdc6bfc29'

  AND DATE(block_timestamp) = '2020-01-01'

  )
SELECT
     block_timestamp
     ,block_number
     ,transaction_hash
     ,log_index
     ,contract_address

    ,parsed.hash AS `hash`
    ,parsed.bidder AS `bidder`
    ,parsed.deposit AS `deposit`
FROM parsed_logs