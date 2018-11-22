WITH tokens_grouped AS (
    SELECT
        address,
        symbol,
        name,
        decimals,
        total_supply,
        ROW_NUMBER() OVER (PARTITION BY address) AS rank
    FROM
        {{DATASET_NAME_RAW}}.tokens)
SELECT
    address,
    symbol,
    name,
    decimals,
    total_supply
FROM tokens_grouped
WHERE tokens_grouped.rank = 1
