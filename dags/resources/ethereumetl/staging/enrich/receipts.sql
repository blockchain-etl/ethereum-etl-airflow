CREATE TABLE IF NOT EXISTS $CHAIN.receipts (
    created_date Date DEFAULT today(),
    block_number Int64,
    block_hash String,
    transaction_hash String,
    transaction_index Int64,
    cumulative_gas_used Int64,
    gas_used Int64,
    contract_address String,
    root String,
    \"status\" Int64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (transaction_hash);

