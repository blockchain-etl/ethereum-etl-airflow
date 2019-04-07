SELECT IF(
(SELECT MAX(number) FROM `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.blocks`) + 1 =
(SELECT COUNT(*) FROM `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.blocks`), 1,
CAST((SELECT 'Total number of blocks except genesis is not equal to last block number {{ds}}') AS INT64))
