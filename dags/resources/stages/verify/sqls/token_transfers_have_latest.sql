SELECT IF(
(SELECT COUNT(*) FROM `bigquery-public-data.blockchain.token_transfers` WHERE DATE(block_timestamp) = '{{ds}}') > 0, 1,
CAST((SELECT 'There are no token transfers on {{ds}}') AS INT64))
