SELECT *
FROM `{{internal_project_id}}.{{dataset_name}}.{{history_table_name}}`
WHERE DATE(block_timestamp) <= '{{ds}}'
UNION ALL
SELECT *
FROM `{{internal_project_id}}.{{dataset_name}}.{{table_name}}`
WHERE DATE(block_timestamp) > '{{ds}}'