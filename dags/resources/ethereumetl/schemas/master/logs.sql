CREATE TABLE IF NOT EXISTS $CHAIN.logs (
    created_date Date DEFAULT today(),
    log_index Int64,
    block_date Date,
    transaction_hash String,
    transaction_index Int64,
    address String,
    \"data\" String,
    topics String,
    block_number Int64,
    block_hash String,
    block_timestamp Int32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(block_date)
ORDER BY (block_number);