merge `{{params.destination_dataset_project_id}}.{{params.destination_dataset_name}}.traces` dest
using {{params.dataset_name_temp}}.{{params.source_table}} source
on false
when not matched and date(block_timestamp) = '{{ds}}' then
insert (
    transaction_hash,
    transaction_index,
    from_address,
    to_address,
    value,
    input,
    output,
    trace_type,
    call_type,
    reward_type,
    gas,
    gas_used,
    subtraces,
    trace_address,
    error,
    status,
    trace_id,
    block_timestamp,
    block_number,
    block_hash
) values (
    transaction_hash,
    transaction_index,
    from_address,
    to_address,
    value,
    input,
    output,
    trace_type,
    call_type,
    reward_type,
    gas,
    gas_used,
    subtraces,
    trace_address,
    error,
    status,
    trace_id,
    block_timestamp,
    block_number,
    block_hash
)
when not matched by source and date(block_timestamp) = '{{ds}}' then
delete
