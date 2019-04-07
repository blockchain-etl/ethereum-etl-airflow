SELECT IF(
(SELECT COUNT(1)
FROM `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.traces` as traces
WHERE trace_type = 'create' AND trace_address IS NULL) =
(SELECT COUNT(*)
FROM `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.transactions` AS transactions
WHERE receipt_contract_address IS NOT NULL) AND
(SELECT COUNT(1)
FROM `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.traces` as traces
WHERE trace_type = 'create' AND to_address IS NOT NULL AND status = 1) =
(SELECT COUNT(*)
FROM `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.contracts` AS contracts
), 1,
CAST((SELECT 'Total number of traces with type create is not equal to number of contracts on {{ds}}') AS INT64))
