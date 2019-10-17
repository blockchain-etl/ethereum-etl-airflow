CREATE TABLE IF NOT EXISTS $CHAIN.contracts (
    created_date Date DEFAULT today(),
    address String,
    bytecode String,
    function_sighashes String,
    is_erc20 UInt8,
    is_erc721 UInt8,
    block_number Int64,
    block_hash String,
    block_timestamp Int32,
    block_date Date,
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(block_date)
ORDER BY (block_number);