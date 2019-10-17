CREATE TABLE IF NOT EXISTS $CHAIN.blocks_$EXECUTION_DATE_NODASH (
    created_date Date DEFAULT today(),
    hash String,
    number Int32,
    parent_hash String,
    \"timestamp\" Int64,
    nonce String,
    sha3_uncles String,
    logs_bloom String,
    transactions_root String,
    state_root String,
    receipts_root String,
    miner String,
    difficulty Int32,
    total_difficulty Int32,
    size Int64,
    extra_data String,
    gas_limit Int64,
    gas_used Int64,
    transaction_count Int32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_date)
ORDER BY (number);