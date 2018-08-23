SELECT IF(
(SELECT COUNT(*) FROM `bigquery-public-data.ethereum_blockchain.transactions` WHERE DATE(block_timestamp) = '{{ds}}') > 0, 1,
CAST((SELECT 'There are no transactions on {{ds}}') AS INT64))
