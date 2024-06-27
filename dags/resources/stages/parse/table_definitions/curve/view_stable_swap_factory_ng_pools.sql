SELECT
  token_address as pool_address,
FROM
  `bigquery-public-data.crypto_ethereum.token_transfers`
WHERE
  from_address = '0x0000000000000000000000000000000000000000'
  AND to_address = '0x6a8cbed756804b16e05e741edabd5cb544ae21bf'
  AND value = '0'
  AND TIMESTAMP_TRUNC(block_timestamp, day) > TIMESTAMP('2023-10-29')