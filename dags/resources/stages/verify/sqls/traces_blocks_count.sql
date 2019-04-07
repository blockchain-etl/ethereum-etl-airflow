SELECT IF(
(SELECT COUNT(DISTINCT(block_number)) FROM `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.traces`
WHERE trace_type = 'reward' AND reward_type = 'block') =
(SELECT COUNT(*) FROM `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.blocks`) - 1, 1,
CAST((SELECT 'Total number of unique blocks in traces is not equal to block count minus 1 on {{ds}}') AS INT64))
