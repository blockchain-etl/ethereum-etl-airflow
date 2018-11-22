SELECT IF((SELECT sum(transaction_count) FROM `{{DESTINATION_DATASET_PROJECT_ID}}.{{DATASET_NAME}}.blocks`) =
(SELECT COUNT(*) FROM `{{DESTINATION_DATASET_PROJECT_ID}}.{{DATASET_NAME}}.transactions`), 1,
CAST((SELECT 'Total number of transactions is not equal to sum of transaction_count in blocks table') AS INT64))
