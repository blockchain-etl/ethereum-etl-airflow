CREATE VIEW $CHAIN.traces_staged_$EXECUTION_DATE_NODASH AS
SELECT
    toUnixTimestamp(now())  AS created_time,
    staged_traces.block_number AS block_number,
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
    blocks_master.hash AS block_hash,
    blocks_master.timestamp AS block_timestamp,
    toDate(blocks_master.timestamp) AS block_date
FROM
    $CHAIN.traces_$EXECUTION_DATE_NODASH AS staged_traces
LEFT JOIN
    $CHAIN.blocks AS blocks_master ON
    (staged_traces.block_number = blocks_master.number);