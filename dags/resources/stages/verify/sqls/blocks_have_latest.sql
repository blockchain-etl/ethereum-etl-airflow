SELECT IF(
(SELECT COUNT(*) FROM `bigquery-public-data.blockchain.blocks` WHERE DATE(timestamp) = '{{ds}}') > 0, 1,
CAST((SELECT 'There are no blocks on {{ds}}') AS INT64))
