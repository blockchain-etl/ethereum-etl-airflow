select if(
(
select count(transaction_hash)
from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.traces`
where trace_address is null and transaction_hash is not null
    and date(block_timestamp) <= '{{ds}}'
) =
(
select count(*)
from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.transactions`
where date(block_timestamp) <= '{{ds}}'
), 1,
cast((select 'Total number of traces with null address is not equal to transaction count on {{ds}}') as int64))
