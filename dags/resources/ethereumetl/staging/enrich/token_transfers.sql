CREATE VIEW $CHAIN.token_transfers_staged_$EXECUTION_DATE_NODASH AS
SELECT
    toUnixTimestamp(now())  AS created_time,
    staged_token_transfers.token_address AS token_address,
    staged_token_transfers.from_address AS from_address,
    staged_token_transfers.to_address AS to_address,
    staged_token_transfers.value AS value,
    staged_token_transfers.transaction_hash AS transaction_hash,
    staged_token_transfers.log_index AS log_index,
    staged_token_transfers.block_number AS block_number,
    blocks_master.hash AS block_hash,
    blocks_master.timestamp AS block_timestamp,
    toDate(blocks_master.timestamp) AS block_date
FROM
    $CHAIN.token_transfers_$EXECUTION_DATE_NODASH AS staged_token_transfers
LEFT JOIN
    $CHAIN.blocks AS blocks_master ON
    (staged_token_transfers.block_number = blocks_master.number);