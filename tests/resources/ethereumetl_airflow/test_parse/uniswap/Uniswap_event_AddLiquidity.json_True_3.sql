SELECT *
FROM `blockchain-etl-internal.ethereum_uniswap.Uniswap_event_AddLiquidity_history`
WHERE DATE(block_timestamp) <= '2020-01-01'
UNION ALL
SELECT *
FROM `blockchain-etl-internal.ethereum_uniswap.Uniswap_event_AddLiquidity`
WHERE DATE(block_timestamp) > '2020-01-01'