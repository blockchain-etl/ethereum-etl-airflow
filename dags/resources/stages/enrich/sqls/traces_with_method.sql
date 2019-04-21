CREATE TEMP FUNCTION
  DECODE_TRANSACTION_INPUT(contractAbi STRING, data STRING)
  RETURNS STRING
  LANGUAGE js AS """

  if (contractAbi == null || contractAbi.length == 0 || data == null || data.length == 0) {
    return null;
  }

  contractAbi = JSON.parse(contractAbi);

  function getKeys(params, key, allowEmpty) {
      var result = [];

      if (!Array.isArray(params)) { throw new Error(`[ethjs-abi] while getting keys, invalid params value ${JSON.stringify(params)}`); }

      for (var i = 0; i < params.length; i++) { // eslint-disable-line
          var value = params[i][key];  // eslint-disable-line
          if (allowEmpty && !value) {
              value = '';
          } else if (typeof(value) !== 'string') {
              throw new Error('[ethjs-abi] while getKeys found invalid ABI data structure, type value not string');
          }
          result.push(value);
      }

      return result;
  }

  function decodeTransactionInputForMethod(method, data) {
      const outputNames = getKeys(method.inputs, 'name', true);
      const outputTypes = getKeys(method.inputs, 'type');

      return abi.decodeParams(outputNames, outputTypes, data, true);
  }

  function findMethodForTransactionInput(contractAbi, data) {
      if (!data || data.length < 10) {
          return null;
      }

      const signature = data.substring(0, 10);

      const matchingFunctions = contractAbi.filter(item => {
          if (item.type === 'function') {
              const itemSignature = abi.encodeSignature(item);
              return itemSignature === signature;
          } else {
              return false;
          }
      });

      if (matchingFunctions.length === 0) {
          return null
      } else {
          return matchingFunctions[0];
      }
  }

  function decodeTransactionInput(method, data) {
      if (!data || data.length < 10) {
          return []
      }

      if (method) {
          const paramData = '0x' + data.substring(10);
          return decodeTransactionInputForMethod(method, paramData);
      } else {
          return null;
      }
  }

  function resultToArray(result) {
      const arr = [];

      for (let i = 0; i < 1024; i++) {
          if (result[i]) {
              arr.push(result[i])
          } else {
              break;
          }
      }
      return arr;
  }

  const method = findMethodForTransactionInput(contractAbi, data);

  if (method) {
    try {
      const res = decodeTransactionInput(method, data);
      return `${method.name}(${resultToArray(res).join(',')})`;
    } catch(err) {
        if (err.name === 'Error' && (err.message.includes('decoding') || err.message.includes('Number'))) {
            return null;
        } else {
            throw err;
        }
    }
  } else {
    return null;
  }
"""
OPTIONS
  ( library="https://storage.googleapis.com/ethlab-183014.appspot.com/ethjs-abi.js" );
SELECT
    traces.transaction_hash,
    traces.transaction_index,
    traces.from_address,
    traces.to_address,
    traces.value,
    traces.input,
    traces.output,
    traces.trace_type,
    traces.call_type,
    traces.reward_type,
    traces.gas,
    traces.gas_used,
    traces.subtraces,
    traces.trace_address,
    traces.error,
    traces.status,
    traces.block_timestamp,
    traces.block_number,
    traces.block_hash,
    if (token_abis.abi is not null and traces.input is not null and length(traces.input) >= 10, DECODE_TRANSACTION_INPUT(token_abis.abi, traces.input), null) as method
FROM crypto_ethereum.traces AS traces
left join graph_database_test.token_abis as token_abis on
    token_abis.address = traces.to_address
where true
    and date(block_timestamp) = '{{ds}}'



