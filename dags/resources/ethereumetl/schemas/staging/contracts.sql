CREATE TABLE IF NOT EXISTS $CHAIN.contracts_$EXECUTION_DATE_NODASH (
    address String,
    bytecode String,
    function_sighashes Array(String),
    is_erc20 UInt8,
    is_erc721 UInt8,
    block_number UInt32
)
ENGINE = MergeTree()
PARTITION BY tuple()
ORDER BY (block_number);