merge `{{params.destination_dataset_project_id}}.{{params.destination_dataset_name}}.token_transfers` dest
using {{params.dataset_name_temp}}.{{params.source_table}} source
on false
when not matched and date(block_timestamp) = '{{ds}}' then
insert (
    token_address,
    from_address,
    to_address,
    value,
    transaction_hash,
    log_index,
    block_timestamp,
    block_number,
    block_hash
) values (
    token_address,
    from_address,
    to_address,
    value,
    transaction_hash,
    log_index,
    block_timestamp,
    block_number,
    block_hash
)
when not matched by source and date(block_timestamp) = '{{ds}}' then
delete
