CREATE VIEW $CHAIN.transactions_staged_$EXECUTION_DATE_NODASH AS
SELECT
    staged_transactions.created_date AS created_date,
    staged_transactions.hash AS hash,
    staged_transactions.nonce AS nonce,
    staged_transactions.transaction_index AS transaction_index,
    staged_transactions.from_address AS from_address,
    staged_transactions.to_address AS to_address,
    staged_transactions.value AS value,
    staged_transactions.input AS input,
    staged_transactions.gas AS gas,
    staged_transactions.gas_price AS gas_price,
    receipts_master.receipt_cumulative_gas_used AS receipt_cumulative_gas_used,
    receipts_master.receipt_gas_used AS receipt_gas_used,
    receipts_master.receipt_contract_address AS receipt_contract_address,
    receipts_master.receipt_root AS receipt_root,
    receipts_master.receipt_status AS receipt_status,
    blocks_master.block_hash AS block_hash,
    blocks_master.block_number AS block_number,
    blocks_master.block_timestamp AS block_timestamp,
    toDate(blocks_master.block_timestamp) AS block_date
FROM
    $CHAIN.transactions_$EXECUTION_DATE_NODASH AS staged_transactions
LEFT JOIN
    staged_transactions.$CHAIN.blocks AS blocks_master ON
    (staged_transactions.block_number = blocks_master.number)
LEFT JOIN
    $CHAIN.receipts AS receipts_master ON
    (staged_transactions.hash = receipts_master.transaction_hash)
