SELECT IF(
(SELECT COUNT(1)
FROM `bigquery-public-data.ethereum_blockchain.traces` as traces
WHERE trace_type = 'create' AND trace_address IS NULL) =
(SELECT COUNT(*)
FROM `bigquery-public-data.ethereum_blockchain.transactions` AS transactions
WHERE receipt_contract_address IS NOT NULL), 1,
CAST((SELECT 'Total number of traces with type create is not equal to number of contracts on {{ds}}') AS INT64))
