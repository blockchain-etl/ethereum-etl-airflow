merge `{{params.destination_dataset_project_id}}.{{params.destination_dataset_name}}.blocks` dest
using {{params.dataset_name_temp}}.{{params.source_table}} source
on false
when not matched and date(timestamp) = '{{ds}}' then
insert (
    timestamp,
    number,
    `hash`,
    parent_hash,
    nonce,
    sha3_uncles,
    logs_bloom,
    transactions_root,
    state_root,
    receipts_root,
    miner,
    difficulty,
    total_difficulty,
    size,
    extra_data,
    gas_limit,
    gas_used,
    transaction_count,
    base_fee_per_gas
) values (
    timestamp,
    number,
    `hash`,
    parent_hash,
    nonce,
    sha3_uncles,
    logs_bloom,
    transactions_root,
    state_root,
    receipts_root,
    miner,
    difficulty,
    total_difficulty,
    size,
    extra_data,
    gas_limit,
    gas_used,
    transaction_count,
    base_fee_per_gas
)
when not matched by source and date(timestamp) = '{{ds}}' then
delete
