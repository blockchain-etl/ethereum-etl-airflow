SELECT IF(
(SELECT COUNT(transaction_hash) FROM `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.traces`
WHERE trace_address IS NULL AND transaction_hash IS NOT NULL) =
(SELECT COUNT(*) FROM `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.transactions`), 1,
CAST((SELECT 'Total number of traces with null address is not equal to transaction count on {{ds}}') AS INT64))
