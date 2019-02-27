SELECT IF(
(SELECT COUNT(1)
FROM `{{DESTINATION_DATASET_PROJECT_ID}}.{{DATASET_NAME}}.contracts`
) =
(SELECT COUNT(DISTINCT address)
FROM `{{DESTINATION_DATASET_PROJECT_ID}}.{{DATASET_NAME}}.contracts`
), 1,
CAST((SELECT 'There are duplicate addresses in contracts table') AS INT64))
