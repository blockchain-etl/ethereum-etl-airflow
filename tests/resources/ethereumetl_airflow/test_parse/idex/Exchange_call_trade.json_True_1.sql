WITH parsed_traces AS
(SELECT
    traces.block_timestamp AS block_timestamp
    ,traces.block_number AS block_number
    ,traces.transaction_hash AS transaction_hash
    ,traces.transaction_index AS transaction_index
    ,traces.trace_address AS trace_address
    ,traces.to_address AS to_address
    ,traces.status AS status
    ,`blockchain-etl-internal.ethereum_idex.parse_Exchange_call_trade`(traces.input) AS parsed
FROM `blockchain-etl-internal.crypto_ethereum_partitioned.traces_by_input_0xef3` AS traces
WHERE to_address IN (

    lower('0x2a0c0dbecc7e4d658f48e01e3fa353f44050c208')

  )
  AND STARTS_WITH(traces.input, '0xef343588')


  -- live


  )
SELECT
     block_timestamp
     ,block_number
     ,transaction_hash
     ,transaction_index
     ,trace_address
     ,to_address
     ,status
     ,parsed.error AS error

    ,parsed.tradeValues AS `tradeValues`

    ,parsed.tradeAddresses AS `tradeAddresses`

    ,parsed.v AS `v`

    ,parsed.rs AS `rs`

FROM parsed_traces