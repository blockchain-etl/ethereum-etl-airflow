SELECT *
FROM `blockchain-etl-internal.ethereum_uniswap.UniswapV2Pair_event_Swap_history`
WHERE DATE(block_timestamp) <= '2020-01-01'
UNION ALL
SELECT *
FROM `blockchain-etl-internal.ethereum_uniswap.UniswapV2Pair_event_Swap`
WHERE DATE(block_timestamp) > '2020-01-01'