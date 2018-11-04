SELECT IF(
(SELECT COUNT(transaction_hash) FROM `ethereum_blockchain.traces`
WHERE trace_address IS NULL) =
(SELECT COUNT(*) FROM `bigquery-public-data.ethereum_blockchain.transactions`), 1,
CAST((SELECT 'Total number of traces with null address is not equal to transaction count on {{ds}}') AS INT64))
