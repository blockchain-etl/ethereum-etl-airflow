WITH parsed_traces AS
(SELECT
    traces.block_timestamp AS block_timestamp
    ,traces.block_number AS block_number
    ,traces.transaction_hash AS transaction_hash
    ,traces.trace_address AS trace_address
    ,traces.status AS status
    ,`blockchain-etl-internal.ethereum_idex.parse_Exchange_call_trade`(traces.input) AS parsed
FROM `bigquery-public-data.crypto_ethereum.traces` AS traces
WHERE to_address IN (

    '0x2a0c0dbecc7e4d658f48e01e3fa353f44050c208'

  )
  AND STARTS_WITH(traces.input, '0xef343588')

  -- pass

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