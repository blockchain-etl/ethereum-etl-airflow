CREATE TABLE IF NOT EXISTS $CHAIN.token_transfers_$EXECUTION_DATE_NODASH (
    token_address String,
    from_address String,
    to_address String,
    value String,
    transaction_hash String,
    log_index UInt32,
    block_number UInt32
)
ENGINE = MergeTree()
PARTITION BY tuple()
ORDER BY (block_number);