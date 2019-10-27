CREATE TABLE IF NOT EXISTS $CHAIN.tokens_$EXECUTION_DATE_NODASH (
    address String,
    symbol String,
    name String,
    decimals String,
    total_supply String,
    block_number UInt32
)
ENGINE = MergeTree()
PARTITION BY tuple()
ORDER BY (block_number);