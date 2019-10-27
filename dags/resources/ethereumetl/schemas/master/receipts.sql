CREATE TABLE IF NOT EXISTS $CHAIN.receipts (
    created_time UInt64 DEFAULT toUnixTimestamp(now()),
    transaction_hash String,
    transaction_index UInt32,
    block_hash String,
    block_number UInt32,
    cumulative_gas_used UInt64,
    gas_used UInt64,
    contract_address String,
    root String,
    \"status\" UInt8,
    block_timestamp UInt64,
    block_date Date
)
ENGINE = ReplacingMergeTree()
PARTITION BY block_date
ORDER BY (block_number, transaction_hash, transaction_index);

