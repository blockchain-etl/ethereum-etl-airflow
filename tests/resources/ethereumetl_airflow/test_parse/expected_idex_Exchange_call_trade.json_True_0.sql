CREATE TEMP FUNCTION
    PARSE_TRACE(data STRING)
    RETURNS STRUCT<`tradeValues` ARRAY<STRING>, `tradeAddresses` ARRAY<STRING>, `v` ARRAY<STRING>, `rs` ARRAY<STRING>, error STRING>
    LANGUAGE js AS """
    var abi = {"constant": false, "inputs": [{"name": "tradeValues", "type": "uint256[8]"}, {"name": "tradeAddresses", "type": "address[4]"}, {"name": "v", "type": "uint8[2]"}, {"name": "rs", "type": "bytes32[4]"}], "name": "trade", "outputs": [{"name": "success", "type": "bool"}], "payable": false, "stateMutability": "nonpayable", "type": "function"};
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
FROM `bigquery-public-data.crypto_ethereum.traces` AS traces
WHERE to_address = '0x2a0c0dbecc7e4d658f48e01e3fa353f44050c208'
  AND STARTS_WITH(traces.input, '0xef343588')

  AND DATE(block_timestamp) <= '2020-01-01'

  )
SELECT
     block_timestamp
     ,block_number
     ,transaction_hash
     ,trace_address
     ,status
     ,parsed.error AS error

    ,parsed.tradeValues AS `tradeValues`

    ,parsed.tradeAddresses AS `tradeAddresses`

    ,parsed.v AS `v`

    ,parsed.rs AS `rs`

FROM parsed_traces