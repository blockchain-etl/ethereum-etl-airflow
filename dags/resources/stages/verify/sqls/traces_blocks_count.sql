SELECT IF(
(SELECT COUNT(DISTINCT(block_number)) FROM `ethereum_blockchain.traces`) =
(SELECT COUNT(*) FROM `bigquery-public-data.ethereum_blockchain.blocks`) - 1, 1,
CAST((SELECT 'Total number of unique blocks in traces is not equal to block count minus 1 on {{ds}}') AS INT64))
