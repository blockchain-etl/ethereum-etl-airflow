SELECT IF(
(SELECT MAX(number) FROM `{{DESTINATION_DATASET_PROJECT_ID}}.{{DATASET_NAME}}.blocks`) + 1 =
(SELECT COUNT(*) FROM `{{DESTINATION_DATASET_PROJECT_ID}}.{{DATASET_NAME}}.blocks`), 1,
CAST((SELECT 'Total number of blocks except genesis is not equal to last block number {{ds}}') AS INT64))
