SELECT IF(
(SELECT COUNT(*) FROM `{{DESTINATION_DATASET_PROJECT_ID}}.{{DATASET_NAME}}.logs` WHERE DATE(block_timestamp) = '{{ds}}') > 0, 1,
CAST((SELECT 'There are no logs on {{ds}}') AS INT64))
