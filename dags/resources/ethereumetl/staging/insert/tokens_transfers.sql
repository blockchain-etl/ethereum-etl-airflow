INSERT INTO $CHAIN.tokens_transfers
SELECT
    staged_tokens_transfers.created_date AS created_date,
    staged_tokens_transfers.token_address AS token_address,
    staged_tokens_transfers.transaction_hash AS transaction_hash,
    staged_tokens_transfers.log_index AS log_index,
    staged_tokens_transfers.from_address AS from_address,
    staged_tokens_transfers.to_address AS to_address,
    staged_tokens_transfers.value AS value,
    staged_tokens_transfers.status AS status,
    staged_tokens_transfers.block_number AS block_number,
    staged_tokens_transfers.block_hash AS block_hash,
    staged_tokens_transfers.block_timestamp AS block_timestamp,
    staged_tokens_transfers.block_date AS block_date,
FROM
    $CHAIN.tokens_transfers_staged_$EXECUTION_DATE_NODASH AS staged_tokens_transfers;
