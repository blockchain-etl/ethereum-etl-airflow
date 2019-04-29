select if(
(
select sum(transaction_count)
from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.blocks`
where date(timestamp) <= '{{ds}}'
) =
(
select count(*)
from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.transactions`
where date(block_timestamp) <= '{{ds}}'
), 1,
cast((select 'Total number of transactions is not equal to sum of transaction_count in blocks table') as int64))
