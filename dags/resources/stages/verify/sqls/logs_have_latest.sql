SELECT IF(
(SELECT COUNT(*) FROM `bigquery-public-data.blockchain.logs` WHERE DATE(block_timestamp) = '{{ds}}') > 0, 1,
CAST((SELECT 'There are no logs on {{ds}}') AS INT64))
