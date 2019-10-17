CREATE TABLE IF NOT EXISTS $CHAIN.transactions (
    created_date Date DEFAULT today(),
    hash String
    nonce Int64,
    transaction_index Int64,
    from_address String,
    to_address String,
    value Int32,
    gas Int64,
    gas_price Int64,
    input String,
    receipt_cumulative_gas_used Int64,
    receipt_gas_used Int64,
    receipt_contract_address String,
    receipt_root String,
    receipt_status Int64,
    block_hash String,
    block_number Int64,
    block_timestamp Int32,
    block_date Date,
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(block_date)
ORDER BY (block_number, hash);