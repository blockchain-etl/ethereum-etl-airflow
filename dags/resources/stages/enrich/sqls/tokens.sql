SELECT
    address,
    symbol,
    name,
    decimals,
    total_supply
FROM {{DATASET_NAME_RAW}}.tokens

