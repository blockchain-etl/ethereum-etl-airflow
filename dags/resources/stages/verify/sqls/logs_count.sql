select if(
(
with blocks_with_wrong_logs_count as (
  select block_number
  from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.logs`
  where date(block_timestamp) <= '{{ds}}'
  group by block_number
  having count(*) - 1 != max(log_index)
)
select count(*)
from blocks_with_wrong_logs_count
) = 0, 1,
cast((select 'The number of logs in a block minus 1 is not equal to max log_index in the block') as int64))

