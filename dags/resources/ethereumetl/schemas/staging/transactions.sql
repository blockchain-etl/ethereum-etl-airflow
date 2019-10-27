CREATE TABLE IF NOT EXISTS $CHAIN.transactions_$EXECUTION_DATE_NODASH (
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
    block_timestamp UInt64
)
ENGINE = MergeTree()
PARTITION BY tuple()
ORDER BY (block_number, hash);