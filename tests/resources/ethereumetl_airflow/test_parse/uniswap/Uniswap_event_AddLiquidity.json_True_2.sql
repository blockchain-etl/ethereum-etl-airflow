WITH parsed_logs AS
(SELECT
    logs.block_timestamp AS block_timestamp
    ,logs.block_number AS block_number
    ,logs.transaction_hash AS transaction_hash
    ,logs.log_index AS log_index
    ,logs.address AS contract_address
    ,`blockchain-etl.ethereum_uniswap_internal.parse_Uniswap_event_AddLiquidity`(logs.data, logs.topics) AS parsed
FROM `bigquery-public-data.crypto_ethereum.logs` AS logs
WHERE address in (

    SELECT exchange FROM `blockchain-etl.ethereum_uniswap.Vyper_contract_event_NewExchange`

  )
  AND topics[SAFE_OFFSET(0)] = '0x06239653922ac7bea6aa2b19dc486b9361821d37712eb796adfd38d81de278ca'

  AND DATE(block_timestamp) <= '2020-01-01'

  )
SELECT
     block_timestamp
     ,block_number
     ,transaction_hash
     ,log_index
     ,contract_address

    ,parsed.provider AS `provider`
    ,parsed.eth_amount AS `eth_amount`
    ,parsed.token_amount AS `token_amount`
FROM parsed_logs