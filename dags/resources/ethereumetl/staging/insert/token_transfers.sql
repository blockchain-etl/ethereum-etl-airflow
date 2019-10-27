INSERT INTO $CHAIN.token_transfers
SELECT
    staged_token_transfers.created_time AS created_time,
    staged_token_transfers.token_address AS token_address,
    staged_token_transfers.from_address AS from_address,
    staged_token_transfers.to_address AS to_address,
    staged_token_transfers.value AS value,
    staged_token_transfers.transaction_hash AS transaction_hash,
    staged_token_transfers.log_index AS log_index,
    staged_token_transfers.block_number AS block_number,
    staged_token_transfers.block_hash AS block_hash,
    staged_token_transfers.block_timestamp AS block_timestamp,
    staged_token_transfers.block_date AS block_date
FROM
    $CHAIN.token_transfers_staged_$EXECUTION_DATE_NODASH AS staged_token_transfers;
