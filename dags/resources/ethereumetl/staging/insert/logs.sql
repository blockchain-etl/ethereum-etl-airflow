INSERT INTO $CHAIN.logs
SELECT
    staged_logs.created_time AS created_time,
    staged_logs.log_index AS log_index,
    staged_logs.transaction_hash AS transaction_hash,
    staged_logs.transaction_index AS transaction_index,
    staged_logs.block_hash AS block_hash,
    staged_logs.block_number AS block_number,
    staged_logs.address AS address,
    staged_logs.data AS data,
    staged_logs.topics AS topics,
    staged_logs.block_timestamp AS block_timestamp,
    staged_logs.block_date AS block_date
FROM
    $CHAIN.logs_staged_$EXECUTION_DATE_NODASH AS staged_logs;