WITH parsed_logs AS
(SELECT
    logs.block_timestamp AS block_timestamp
    ,logs.block_number AS block_number
    ,logs.transaction_hash AS transaction_hash
    ,logs.log_index AS log_index
    ,logs.address AS contract_address
    ,`blockchain-etl-internal.ethereum_dydx.parse_SoloMargin_event_LogTrade`(logs.data, logs.topics) AS parsed
FROM `blockchain-etl-internal.crypto_ethereum_partitioned.logs_by_topic_0x54d` AS logs
WHERE

  address in ('0x1e0447b19bb6ecfdae1e4ae1694b0c3659614e4e')

  AND topics[SAFE_OFFSET(0)] = '0x54d4cc60cf7d570631cc1a58942812cb0fc461713613400f56932040c3497d19'


  -- live


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
WHERE parsed IS NOT NULL