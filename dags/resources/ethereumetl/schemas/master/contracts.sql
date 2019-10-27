CREATE TABLE IF NOT EXISTS $CHAIN.contracts (
    created_time UInt64 DEFAULT toUnixTimestamp(now()),
    address String,
    bytecode String,
    function_sighashes Array(String),
    is_erc20 UInt8,
    is_erc721 UInt8,
    block_number UInt32,
    block_hash String,
    block_timestamp UInt64,
    block_date Date
)
ENGINE = ReplacingMergeTree()
PARTITION BY block_date
ORDER BY (block_number, address);