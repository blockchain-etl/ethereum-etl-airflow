select if(
(
select count(*)
from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.logs`
where date(block_timestamp) <= '{{ds}}'
group by block_number
having count(*) - 1 != max(log_index)
) = 0, 1,
cast((select 'The number of logs in a block minus 1 is not equal to max log_index in the block') as int64))

