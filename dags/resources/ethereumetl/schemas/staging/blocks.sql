CREATE TABLE IF NOT EXISTS $CHAIN.blocks_$EXECUTION_DATE_NODASH (
    number UInt32,
    hash String,
    parent_hash String,
    nonce String,
    sha3_uncles String,
    logs_bloom String,
    transactions_root String,
    state_root String,
    receipts_root String,
    miner String,
    difficulty UInt32,
    total_difficulty UInt32,
    size UInt64,
    extra_data String,
    gas_limit UInt64,
    gas_used UInt64,
    \"timestamp\" UInt64,
    transaction_count UInt32
)
ENGINE = MergeTree()
PARTITION BY tuple()
ORDER BY (number);