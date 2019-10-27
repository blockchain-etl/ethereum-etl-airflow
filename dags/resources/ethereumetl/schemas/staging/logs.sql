CREATE TABLE IF NOT EXISTS $CHAIN.logs_$EXECUTION_DATE_NODASH (
    log_index UInt32,
    transaction_hash String,
    transaction_index UInt32,
    block_hash String,
    block_number UInt32,
    address String,
    \"data\" String,
    topics Array(String)
)
ENGINE = MergeTree()
PARTITION BY tuple()
ORDER BY (block_number);