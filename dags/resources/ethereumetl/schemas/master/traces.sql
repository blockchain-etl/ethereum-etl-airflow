CREATE TABLE IF NOT EXISTS $CHAIN.traces (
    created_time UInt64 DEFAULT toUnixTimestamp(now()),
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
    gas UInt64,
    gas_used UInt64,
    subtraces UInt32,
    trace_address String,
    error String,
    \"status\" UInt8,
    block_hash String,
    block_timestamp UInt64,
    block_date Date
)
ENGINE = ReplacingMergeTree()
PARTITION BY block_date
ORDER BY (block_number, transaction_hash, transaction_index, trace_address);