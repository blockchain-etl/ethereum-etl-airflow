CREATE TABLE IF NOT EXISTS $CHAIN.tokens (
    created_date Date DEFAULT today(),
    address String,
    symbol String,
    name String,
    decimals String,
    total_supply String,
    block_number Int64,
    block_hash String,
    block_timestamp Int32,
    block_date Date,
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(block_date)
ORDER BY (block_number);