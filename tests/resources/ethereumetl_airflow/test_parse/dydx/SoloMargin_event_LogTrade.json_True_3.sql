SELECT *
FROM `blockchain-etl-internal.ethereum_dydx.SoloMargin_event_LogTrade_history`
WHERE DATE(block_timestamp) <= '2020-01-01'
UNION ALL
SELECT *
FROM `blockchain-etl-internal.ethereum_dydx.SoloMargin_event_LogTrade`
WHERE DATE(block_timestamp) > '2020-01-01'