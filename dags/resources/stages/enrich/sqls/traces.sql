WITH traces_with_status AS (
    -- Find all nested traces of failed traces
    WITH nested_failed_traces AS (
        SELECT distinct child.transaction_hash, child.trace_address
        FROM {{DATASET_NAME_RAW}}.traces parent
        JOIN {{DATASET_NAME_RAW}}.traces child
        ON (parent.trace_address IS NULL OR starts_with(child.trace_address, concat(parent.trace_address, ',')))
        AND child.transaction_hash = parent.transaction_hash
        where parent.trace_type IN ('call', 'create')
        AND parent.error IS NOT NULL
    )
    SELECT traces.*, if((traces.error IS NOT NULL or nested_failed_traces.trace_address IS NOT NULL), 0, 1) AS status
    FROM {{DATASET_NAME_RAW}}.traces AS traces
    LEFT JOIN nested_failed_traces ON nested_failed_traces.transaction_hash = traces.transaction_hash
    AND nested_failed_traces.trace_address = traces.trace_address
)
SELECT
    traces.transaction_hash,
    traces.transaction_index,
    traces.from_address,
    traces.to_address,
    traces.value,
    traces.input,
    traces.output,
    traces.trace_type,
    traces.call_type,
    traces.reward_type,
    traces.gas,
    traces.gas_used,
    traces.subtraces,
    traces.trace_address,
    traces.error,
    traces.status,
    TIMESTAMP_SECONDS(blocks.timestamp) AS block_timestamp,
    blocks.number AS block_number,
    blocks.hash AS block_hash
FROM {{DATASET_NAME_RAW}}.blocks AS blocks
    JOIN traces_with_status AS traces ON blocks.number = traces.block_number


