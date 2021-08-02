SELECT
    transactions.hash,
    transactions.nonce,
    transactions.transaction_index,
    transactions.from_address,
    transactions.to_address,
    transactions.value,
    transactions.gas,
    transactions.gas_price,
    transactions.input,
    receipts.cumulative_gas_used AS receipt_cumulative_gas_used,
    receipts.gas_used AS receipt_gas_used,
    receipts.contract_address AS receipt_contract_address,
    receipts.root AS receipt_root,
    receipts.status AS receipt_status,
    TIMESTAMP_SECONDS(blocks.timestamp) AS block_timestamp,
    blocks.number AS block_number,
    blocks.hash AS block_hash,
    transactions.max_fee_per_gas,
    transactions.max_priority_fee_per_gas,
    transactions.transaction_type,
    receipts.effective_gas_price as receipt_effective_gas_price
FROM {{params.dataset_name_raw}}.blocks AS blocks
    JOIN {{params.dataset_name_raw}}.transactions AS transactions ON blocks.number = transactions.block_number
    JOIN {{params.dataset_name_raw}}.receipts AS receipts ON transactions.hash = receipts.transaction_hash
where true
    {% if not params.load_all_partitions %}
    and date(timestamp_seconds(blocks.timestamp)) = '{{ds}}'
    {% endif %}
