SELECT IF(
(SELECT COUNT(transaction_hash) FROM `bigquery-public-data.blockchain.traces`
WHERE trace_address IS NULL AND transaction_hash IS NOT NULL) =
(SELECT COUNT(*) FROM `bigquery-public-data.blockchain.transactions`), 1,
CAST((SELECT 'Total number of traces with null address is not equal to transaction count on {{ds}}') AS INT64))
