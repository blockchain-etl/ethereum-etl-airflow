SELECT IF(
(SELECT COUNT(DISTINCT(block_number)) FROM `{{DESTINATION_DATASET_PROJECT_ID}}.{{DATASET_NAME}}.traces`
WHERE trace_type = 'reward' AND reward_type = 'block') =
(SELECT COUNT(*) FROM `{{DESTINATION_DATASET_PROJECT_ID}}.{{DATASET_NAME}}.blocks`) - 1, 1,
CAST((SELECT 'Total number of unique blocks in traces is not equal to block count minus 1 on {{ds}}') AS INT64))
