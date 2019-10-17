INSERT INTO $CHAIN.logs
SELECT
    staged_logs.created_date AS created_date,
    staged_logs.log_index AS log_index,
    staged_logs.block_number AS block_number,
    staged_logs.block_hash AS block_hash,
    staged_logs.block_timestamp AS block_timestamp,
    staged_logs.block_date AS block_date,
    staged_logs.transaction_hash String,
    staged_logs.transaction_index Int64,
    staged_logs.address AS address,
    staged_logs.data AS data,
    staged_logs.topics AS topics
FROM
    $CHAIN.logs_staged_$EXECUTION_DATE_NODASH AS staged_logs
