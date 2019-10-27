INSERT INTO $CHAIN.receipts
SELECT
    staged_receipts.created_time AS created_time,
    staged_receipts.transaction_hash AS transaction_hash,
    staged_receipts.transaction_index AS transaction_index,
    staged_receipts.block_hash AS block_hash,
    staged_receipts.block_number AS block_number,
    staged_receipts.cumulative_gas_used AS cumulative_gas_used,
    staged_receipts.gas_used AS gas_used,
    staged_receipts.contract_address AS contract_address,
    staged_receipts.root AS root,
    staged_receipts.status AS status,
    staged_receipts.block_timestamp AS block_timestamp,
    staged_receipts.block_date AS block_date
FROM
    $CHAIN.receipts_staged_$EXECUTION_DATE_NODASH AS staged_receipts;

