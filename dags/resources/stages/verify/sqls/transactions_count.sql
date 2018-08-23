SELECT IF((SELECT sum(transaction_count) FROM `bigquery-public-data.ethereum_blockchain.blocks`) =
(SELECT COUNT(*) FROM `bigquery-public-data.ethereum_blockchain.transactions`), 1,
CAST((SELECT 'Total number of transactions is not equal to sum of transaction_count in blocks table') AS INT64))
