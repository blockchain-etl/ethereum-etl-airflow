SELECT
    logs.log_index,
    logs.transaction_hash,
    logs.transaction_index,
    logs.address,
    logs.data,
    logs.topics,
    TIMESTAMP_SECONDS(blocks.timestamp) AS block_timestamp,
    blocks.number AS block_number,
    blocks.hash AS block_hash
FROM {{DATASET_NAME_RAW}}.blocks AS blocks
    JOIN {{DATASET_NAME_RAW}}.logs AS logs ON blocks.number = logs.block_number
