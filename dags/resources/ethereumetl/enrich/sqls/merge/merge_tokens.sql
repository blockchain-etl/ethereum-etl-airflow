merge `{{params.destination_dataset_project_id}}.{{params.destination_dataset_name}}.tokens` dest
using {{params.dataset_name_temp}}.{{params.source_table}} source
on false
when not matched and date(block_timestamp) = '{{ds}}' then
insert (
    address,
    symbol,
    name,
    decimals,
    total_supply,
    block_timestamp,
    block_number,
    block_hash
) values (
    address,
    symbol,
    name,
    decimals,
    total_supply,
    block_timestamp,
    block_number,
    block_hash
)
when not matched by source and date(block_timestamp) = '{{ds}}' then
delete
