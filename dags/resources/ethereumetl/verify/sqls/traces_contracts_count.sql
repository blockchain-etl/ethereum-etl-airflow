select if(
(
select count(1)
from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.traces` as traces
where trace_type = 'create' and trace_address is null
    and date(block_timestamp) <= '{{ds}}'
) =
(
select count(*)
from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.transactions` as transactions
where receipt_contract_address is not null
    and date(block_timestamp) <= '{{ds}}'
) and
(
select count(1)
from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.traces` as traces
where trace_type = 'create' and to_address is not null and status = 1
    and date(block_timestamp) <= '{{ds}}'
) =
(
select count(*)
from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.contracts` as contracts
where date(block_timestamp) <= '{{ds}}'
), 1,
cast((select 'Total number of traces with type create is not equal to number of contracts on {{ds}}') as int64))
