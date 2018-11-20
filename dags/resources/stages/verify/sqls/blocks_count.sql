SELECT IF(
(SELECT MAX(number) FROM `bigquery-public-data.blockchain.blocks`) + 1 =
(SELECT COUNT(*) FROM `bigquery-public-data.blockchain.blocks`), 1,
CAST((SELECT 'Total number of blocks except genesis is not equal to last block number {{ds}}') AS INT64))
