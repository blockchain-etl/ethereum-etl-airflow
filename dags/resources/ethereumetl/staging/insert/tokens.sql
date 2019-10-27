INSERT INTO $CHAIN.tokens
SELECT
      staged_tokens.created_time AS created_time,
      staged_tokens.address AS address,
      staged_tokens.symbol AS symbol,
      staged_tokens.name AS name,
      staged_tokens.decimals AS decimals,
      staged_tokens.total_supply AS total_supply,
      staged_tokens.block_number AS block_number,
      staged_tokens.block_hash AS block_hash,
      staged_tokens.block_timestamp AS block_timestamp,
      staged_tokens.block_date AS block_date
FROM
    $CHAIN.tokens_staged_$EXECUTION_DATE_NODASH AS staged_tokens;