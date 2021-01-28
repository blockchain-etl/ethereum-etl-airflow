WITH tokens AS (
    -- Deduplicate first since the tokens table might have duplicate entries due to CREATE2 https://medium.com/@jason.carver/defend-against-wild-magic-in-the-next-ethereum-upgrade-b008247839d2
    SELECT 
        address,
        ANY_VALUE(symbol) AS symbol,
        ANY_VALUE(name) AS name,
        ANY_VALUE(decimals) AS decimals,
    FROM `bigquery-public-data.crypto_ethereum.tokens`
    GROUP BY address
)
SELECT 
    address,
    COALESCE(am.symbol, tokens.symbol) AS symbol,
    COALESCE(am.name, tokens.name) AS name,
    COALESCE(am.decimals, tokens.decimals) AS decimals,
FROM
  `blockchain-etl-internal.common.token_amendments` AS am
FULL OUTER JOIN
  tokens
USING(address)