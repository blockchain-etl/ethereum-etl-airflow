WITH contracts_grouped AS (
    SELECT
        contracts.address,
        contracts.function_sighashes,
        contracts.is_erc20,
        contracts.is_erc721,
        ROW_NUMBER() OVER (PARTITION BY address) AS rank
    FROM {{DATASET_NAME_RAW}}.contracts AS contracts
)
SELECT
    contracts.address,
    traces.output as bytecode,
    contracts.function_sighashes,
    contracts.is_erc20,
    contracts.is_erc721,
    traces.block_timestamp,
    traces.block_number,
    traces.block_hash
FROM contracts_grouped AS contracts
    JOIN `{{DESTINATION_DATASET_PROJECT_ID}}.{{DATASET_NAME}}.traces` AS traces ON traces.to_address = contracts.address
        AND traces.trace_type = 'create' AND traces.status = 1
WHERE contracts.rank = 1
