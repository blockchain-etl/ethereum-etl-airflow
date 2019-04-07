SELECT
    address,
    symbol,
    name,
    decimals,
    total_supply,
    TIMESTAMP_SECONDS(blocks.timestamp) AS block_timestamp,
    blocks.number AS block_number,
    blocks.hash AS block_hash
FROM {{DATASET_NAME_RAW}}.blocks AS blocks
    JOIN {{DATASET_NAME_RAW}}.tokens AS tokens ON blocks.number = tokens.block_number

