INSERT INTO $CHAIN.traces
SELECT
    staged_traces.created_date AS created_date,
    staged_traces.transaction_hash AS transaction_hash,
    staged_traces.transaction_index AS transaction_index,
    staged_traces.from_address AS from_address,
    staged_traces.to_address AS to_address,
    staged_traces.value AS value,
    staged_traces.input AS input,
    staged_traces.output AS output,
    staged_traces.trace_type AS trace_type,
    staged_traces.call_type AS call_type,
    staged_traces.reward_type AS reward_type,
    staged_traces.gas AS gas,
    staged_traces.gas_used AS gas_used,
    staged_traces.subtraces AS subtraces,
    staged_traces.trace_address AS trace_address,
    staged_traces.error AS error,
    staged_traces.status AS status,
    staged_traces.block_number AS block_number,
    staged_traces.block_hash AS block_hash,
    staged_traces.block_timestamp AS block_timestamp,
    staged_traces.block_date AS block_date
FROM
    $CHAIN.traces_staged_$EXECUTION_DATE_NODASH AS staged_traces;