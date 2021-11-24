WITH parsed_logs AS
(SELECT
    logs.block_timestamp AS block_timestamp
    ,logs.block_number AS block_number
    ,logs.transaction_hash AS transaction_hash
    ,logs.log_index AS log_index
    ,logs.address AS contract_address
    ,`blockchain-etl-internal.ethereum_uniswap.parse_UniswapV2Pair_event_Swap`(logs.data, logs.topics) AS parsed
FROM `blockchain-etl-internal.crypto_ethereum_partitioned.logs_by_date_2020_01_01` AS logs
WHERE
  
  address in (SELECT pair FROM `blockchain-etl.ethereum_uniswap.UniswapV2Factory_event_PairCreated`)
  
  AND topics[SAFE_OFFSET(0)] = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822'


  AND DATE(block_timestamp) = '2020-01-01'
  AND _topic_partition_index = MOD(ABS(FARM_FINGERPRINT('0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822')), 3999)
  

  )
SELECT
     block_timestamp
     ,block_number
     ,transaction_hash
     ,log_index
     ,contract_address
     
    ,parsed.sender AS `sender`
    ,parsed.amount0In AS `amount0In`
    ,parsed.amount1In AS `amount1In`
    ,parsed.amount0Out AS `amount0Out`
    ,parsed.amount1Out AS `amount1Out`
    ,parsed.to AS `to`
FROM parsed_logs
WHERE parsed IS NOT NULL