merge `{{params.destination_dataset_project_id}}.{{params.destination_dataset_name}}.logs` dest
using {{params.dataset_name_temp}}.{{params.source_table}} source
on false
when not matched and date(block_timestamp) = '{{ds}}' then
insert (
    log_index,
    transaction_hash,
    transaction_index,
    address,
    data,
    topics,
    block_timestamp,
    block_number,
    block_hash
) values (
    log_index,
    transaction_hash,
    transaction_index,
    address,
    data,
    topics,
    block_timestamp,
    block_number,
    block_hash
)
when not matched by source and date(block_timestamp) = '{{ds}}' then
delete
