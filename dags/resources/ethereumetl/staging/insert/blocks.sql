INSERT INTO $CHAIN.blocks
SELECT
    staged_blocks.created_date AS created_date,
    staged_blocks.hash  AS hash,
    staged_blocks.number  AS number,
    staged_blocks.parent_hash AS parent_hash,
    staged_blocks.timestamp  AS timestamp,
    staged_blocks.date AS \"date\",
    staged_blocks.date_time AS date_time,
    staged_blocks.nonce AS nonce,
    staged_blocks.sha3_uncles AS sha3_uncles,
    staged_blocks.logs_bloom AS logs_bloom,
    staged_blocks.transactions_root AS transactions_root,
    staged_blocks.state_root AS state_root,
    staged_blocks.receipts_root AS receipts_root,
    staged_blocks.miner AS miner,
    staged_blocks.difficulty AS difficulty,
    staged_blocks.total_difficulty AS total_difficulty,
    staged_blocks.size AS size,
    staged_blocks.extra_data AS extra_data,
    staged_blocks.gas_limit AS gas_limit,
    staged_blocks.gas_used AS gas_used,
    staged_blocks.transaction_count AS transaction_count
FROM
    $CHAIN.blocks_staged_$EXECUTION_DATE_NODASH AS staged_blocks;