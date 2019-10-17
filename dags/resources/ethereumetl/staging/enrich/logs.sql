CREATE VIEW $CHAIN.logs_staged_$EXECUTION_DATE_NODASH AS
SELECT
    staged_logs.created_date AS created_date,
    staged_logs.log_index AS log_index,
    staged_logs.transaction_hash String,
    staged_logs.transaction_index Int64,
    staged_logs.address AS address,
    staged_logs.data AS data,
    staged_logs.topics AS topics,
    blocks_master.block_number AS block_number,
    blocks_master.block_hash AS block_hash,
    blocks_master.block_timestamp AS block_timestamp,
    toDate(blocks_master.block_timestamp) AS block_date
FROM
    $CHAIN.logs_$EXECUTION_DATE_NODASH AS staged_logs
LEFT JOIN
    $CHAIN.blocks AS blocks_master ON
    (staged_logs.block_number = blocks_master.number);