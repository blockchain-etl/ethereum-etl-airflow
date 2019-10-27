CREATE TABLE IF NOT EXISTS $CHAIN.token_transfers (
    created_time UInt64 DEFAULT toUnixTimestamp(now()),
    token_address String,
    from_address String,
    to_address String,
    value String,
    transaction_hash String,
    log_index UInt32,
    block_number UInt32,
    block_hash Int64,
    block_timestamp UInt64,
    block_date Date
)
ENGINE = ReplacingMergeTree()
PARTITION BY block_date
ORDER BY (block_number, transaction_hash, log_index);