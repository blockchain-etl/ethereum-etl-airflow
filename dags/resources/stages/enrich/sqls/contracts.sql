SELECT
    contracts.address,
    contracts.bytecode,
    contracts.function_sighashes,
    contracts.is_erc20,
    contracts.is_erc721,
    TIMESTAMP_SECONDS(blocks.timestamp) AS block_timestamp,
    blocks.number AS block_number,
    blocks.hash AS block_hash
FROM {{DATASET_NAME_RAW}}.contracts AS contracts
    JOIN {{DATASET_NAME_RAW}}.traces AS traces ON traces.to_address = contracts.address
        AND traces.trace_type = 'create'
    JOIN {{DATASET_NAME_RAW}}.blocks AS blocks ON blocks.number = traces.block_number
