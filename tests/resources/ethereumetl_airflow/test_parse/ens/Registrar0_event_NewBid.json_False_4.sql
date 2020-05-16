SELECT *
FROM `blockchain-etl-internal.ethereum_ens.Registrar0_event_NewBid_history`
WHERE DATE(block_timestamp) <= '2020-01-01'
UNION ALL
SELECT *
FROM `blockchain-etl-internal.ethereum_ens.Registrar0_event_NewBid`
WHERE DATE(block_timestamp) > '2020-01-01'