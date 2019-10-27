CREATE VIEW $CHAIN.logs_staged_$EXECUTION_DATE_NODASH AS
SELECT
    toUnixTimestamp(now())  AS created_time,
    staged_logs.log_index AS log_index,
    staged_logs.transaction_hash AS transaction_hash,
    staged_logs.transaction_index AS transaction_index,
    staged_logs.block_hash AS block_hash,
    staged_logs.number AS block_number,
    staged_logs.address AS address,
    staged_logs.data AS data,
    staged_logs.topics AS topics,
    blocks_master.timestamp AS block_timestamp,
    toDate(blocks_master.timestamp) AS block_date
FROM
    $CHAIN.logs_$EXECUTION_DATE_NODASH AS staged_logs
LEFT JOIN
    $CHAIN.blocks AS blocks_master ON
    (staged_logs.block_number = blocks_master.number);