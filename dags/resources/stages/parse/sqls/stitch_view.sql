select *
from `{{destination_project_id}}.{{internal_dataset_name}}.{{history_table_name}}`
where date(block_timestamp) <= '{{ds}}'
union all
select *
from `{{destination_project_id}}.{{internal_dataset_name}}.{{table_name}}`
where date(block_timestamp) > '{{ds}}'