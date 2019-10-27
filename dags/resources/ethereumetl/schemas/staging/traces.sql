CREATE TABLE IF NOT EXISTS $CHAIN.traces_$EXECUTION_DATE_NODASH (
    block_number UInt32,
    transaction_hash String,
    transaction_index UInt32,
    from_address String,
    to_address String,
    value UInt64,
    input String,
    output String,
    trace_type String,
    call_type String,
    reward_type String,
    gas Int64,
    gas_used Int64,
    subtraces UInt32,
    trace_address String,
    error String,
    \"status\" UInt8
)
ENGINE = MergeTree()
PARTITION BY tuple()
ORDER BY (block_number);