CREATE TABLE IF NOT EXISTS $CHAIN.traces (
    created_date Date DEFAULT today(),
    transaction_hash String,
    transaction_index Int64,
    from_address String,
    to_address String,
    value Int32,
    input String,
    output String,
    trace_type String,
    call_type String,
    reward_type String,
    gas Int64,
    gas_used Int64,
    subtraces Int64,
    trace_address String,
    error String,
    \"status\" Int64
    block_number Int64,
    block_hash String,
    block_timestamp int32,
    block_date Date,
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(block_date)
ORDER BY (block_number);