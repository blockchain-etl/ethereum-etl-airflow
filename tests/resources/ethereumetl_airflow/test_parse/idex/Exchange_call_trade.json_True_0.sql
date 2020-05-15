CREATE OR REPLACE FUNCTION
    `blockchain-etl-internal.ethereum_idex.parse_Exchange_call_trade`(data STRING)
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