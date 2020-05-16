SELECT *
FROM `blockchain-etl-internal.ethereum_idex.Exchange_call_trade_history`
WHERE DATE(block_timestamp) <= '2020-01-01'
UNION ALL
SELECT *
FROM `blockchain-etl-internal.ethereum_idex.Exchange_call_trade`
WHERE DATE(block_timestamp) > '2020-01-01'