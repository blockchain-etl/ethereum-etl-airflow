CREATE TABLE IF NOT EXISTS $CHAIN.receipts_$EXECUTION_DATE_NODASH (
    transaction_hash String,
    transaction_index UInt32,
    block_hash String,
    block_number UInt32,
    cumulative_gas_used UInt64,
    gas_used UInt64,
    contract_address String,
    root String,
    \"status\" UInt8
)
ENGINE = MergeTree()
PARTITION BY tuple()
ORDER BY block_hash;

