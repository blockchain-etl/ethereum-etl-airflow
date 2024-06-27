merge `{{params.destination_dataset_project_id}}.{{params.destination_dataset_name}}.transactions` dest
using {{params.dataset_name_temp}}.{{params.source_table}} source
on false
when not matched and date(block_timestamp) = '{{ds}}' then
insert (
    `hash`,
    nonce,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    block_timestamp,
    block_number,
    block_hash,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_effective_gas_price,
    max_fee_per_blob_gas,
    blob_versioned_hashes,
    receipt_blob_gas_price,
    receipt_blob_gas_used
) values (
    `hash`,
    nonce,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    block_timestamp,
    block_number,
    block_hash,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_effective_gas_price,
    max_fee_per_blob_gas,
    blob_versioned_hashes,
    receipt_blob_gas_price,
    receipt_blob_gas_used
)
when not matched by source and date(block_timestamp) = '{{ds}}' then
delete
