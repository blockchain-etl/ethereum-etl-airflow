SELECT IF(
(SELECT COUNT(*) FROM `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.transactions` WHERE DATE(block_timestamp) = '{{ds}}') > 0, 1,
CAST((SELECT 'There are no transactions on {{ds}}') AS INT64))
