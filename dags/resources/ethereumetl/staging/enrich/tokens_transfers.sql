CREATE VIEW $CHAIN.tokens_transfers_staged_$EXECUTION_DATE_NODASH AS
SELECT
    staged_tokens_transfers.created_date AS created_date,
    staged_tokens_transfers.token_address AS token_address,
    staged_tokens_transfers.transaction_hash AS transaction_hash,
    staged_tokens_transfers.log_index AS log_index,
    staged_tokens_transfers.from_address AS from_address,
    staged_tokens_transfers.to_address AS to_address,
    staged_tokens_transfers.value AS value,
    staged_tokens_transfers.status AS status,
    blocks_master.block_number AS block_number,
    blocks_master.block_hash AS block_hash,
    blocks_master.block_timestamp AS block_timestamp,
    toDate(blocks_master.block_timestamp) AS block_date
FROM
    $CHAIN.tokens_transfers_$EXECUTION_DATE_NODASH AS staged_tokens_transfers
LEFT JOIN
    $CHAIN.blocks AS blocks_master ON
    (staged_tokens_transfers.block_number = blocks_master.number);