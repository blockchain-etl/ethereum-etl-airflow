CREATE TABLE IF NOT EXISTS $CHAIN.tokens (
    created_time UInt64 DEFAULT toUnixTimestamp(now()),
    address String,
    symbol String,
    name String,
    decimals String,
    total_supply String,
    block_number UInt32,
    block_hash String,
    block_timestamp UInt64,
    block_date Date
)
ENGINE = ReplacingMergeTree()
PARTITION BY block_date
ORDER BY (block_number, address);