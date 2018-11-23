SELECT IF(
(SELECT COUNT(*) FROM `{{DESTINATION_DATASET_PROJECT_ID}}.{{DATASET_NAME}}.transactions` WHERE DATE(block_timestamp) = '{{ds}}') > 0, 1,
CAST((SELECT 'There are no transactions on {{ds}}') AS INT64))
