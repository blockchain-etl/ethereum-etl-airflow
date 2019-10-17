CREATE VIEW $CHAIN.tokens_staged_$EXECUTION_DATE_NODASH AS
SELECT
    staged_tokens.created_date AS created_date,
    staged_tokens.address AS address,
    staged_tokens.symbol AS symbol,
    staged_tokens.name AS name,
    staged_tokens.decimals AS decimals,
    staged_tokens.total_supply AS total_supply,
    blocks_master.block_number AS block_number,
    blocks_master.block_hash AS block_hash,
    blocks_master.block_timestamp AS block_timestamp,
    toDate(blocks_master.block_timestamp) AS block_date
FROM
    $CHAIN.tokens_$EXECUTION_DATE_NODASH AS staged_tokens
LEFT JOIN
    $CHAIN.blocks AS blocks_master ON
    (staged_tokens.block_number = blocks_master.number);