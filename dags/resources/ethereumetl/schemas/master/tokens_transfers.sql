CREATE TABLE IF NOT EXISTS $CHAIN.token_transfers (
    created_date Date DEFAULT today(),
    token_address String,
    from_address String,
    to_address String,
    value String,
    transaction_hash String,
    log_index Int64,
    block_number Int64,
    block_hash Int64,
    block_timestamp Int32,
    block_date Date
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(block_date)
ORDER BY (block_number);