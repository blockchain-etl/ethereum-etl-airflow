WITH parsed_logs AS
(SELECT
    logs.block_timestamp AS block_timestamp
    ,logs.block_number AS block_number
    ,logs.transaction_hash AS transaction_hash
    ,logs.log_index AS log_index
    ,logs.address AS contract_address
    ,`blockchain-etl-internal.ethereum_ens.parse_Registrar0_event_NewBid`(logs.data, logs.topics) AS parsed
FROM `blockchain-etl-internal.crypto_ethereum_partitioned.logs_by_date_2020_01_01` AS logs
WHERE

  address in (lower('0x6090a6e47849629b7245dfa1ca21d94cd15878ef'))

  AND topics[SAFE_OFFSET(0)] = '0xb556ff269c1b6714f432c36431e2041d28436a73b6c3f19c021827bbdc6bfc29'


  AND DATE(block_timestamp) = '2020-01-01'
  AND _topic_partition_index = MOD(ABS(FARM_FINGERPRINT('0xb556ff269c1b6714f432c36431e2041d28436a73b6c3f19c021827bbdc6bfc29')), 3999)


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
WHERE parsed IS NOT NULL