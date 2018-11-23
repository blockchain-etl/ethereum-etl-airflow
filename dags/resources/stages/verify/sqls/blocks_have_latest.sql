SELECT IF(
(SELECT COUNT(*) FROM `{{DESTINATION_DATASET_PROJECT_ID}}.{{DATASET_NAME}}.blocks` WHERE DATE(timestamp) = '{{ds}}') > 0, 1,
CAST((SELECT 'There are no blocks on {{ds}}') AS INT64))
