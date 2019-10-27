CREATE TABLE IF NOT EXISTS $CHAIN.transactions (
    created_time UInt64 DEFAULT toUnixTimestamp(now()),
    hash String,
    nonce UInt64,
    block_hash String,
    block_number UInt32,
    transaction_index UInt32,
    from_address String,
    to_address String,
    value UInt64,
    gas UInt64,
    gas_price UInt64,
    input String,
    receipt_cumulative_gas_used UInt64,
    receipt_gas_used UInt64,
    receipt_contract_address String,
    receipt_root String,
    receipt_status UInt8,
    block_timestamp UInt64,
    block_date Date
)
ENGINE = ReplacingMergeTree()
PARTITION BY block_date
ORDER BY (block_number, hash);