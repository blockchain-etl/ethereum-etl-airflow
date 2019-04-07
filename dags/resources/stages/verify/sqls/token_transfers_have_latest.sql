SELECT IF(
(SELECT COUNT(*) FROM `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.token_transfers` WHERE DATE(block_timestamp) = '{{ds}}') > 0, 1,
CAST((SELECT 'There are no token transfers on {{ds}}') AS INT64))
