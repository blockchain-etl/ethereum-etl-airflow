select *
from `{{internal_project_id}}.{{dataset_name}}.{{history_table_name}}`
where date(block_timestamp) <= '{{ds}}'
union all
select *
from `{{internal_project_id}}.{{dataset_name}}.{{table_name}}`
where date(block_timestamp) > '{{ds}}'