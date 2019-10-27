CREATE TABLE IF NOT EXISTS $CHAIN.logs (
    created_time UInt32 DEFAULT toUnixTimestamp(now()),
    log_index UInt32,
    transaction_hash String,
    transaction_index UInt32,
    block_hash String,
    block_number UInt32,
    address String,
    \"data\" String,
    topics Array(String),
    block_timestamp UInt64,
    block_date Date
)
ENGINE = ReplacingMergeTree()
PARTITION BY block_date
ORDER BY (block_number, transaction_hash, log_index);