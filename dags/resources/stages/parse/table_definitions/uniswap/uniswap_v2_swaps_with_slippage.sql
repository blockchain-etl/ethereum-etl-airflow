WITH v2_swaps as (
SELECT s.block_timestamp, s.block_number, s.transaction_hash, s.log_index, s.contract_address, s.sender, s.to, p.token0, p.token1, 0.003 as pool_fee,
CASE WHEN CAST(amount0In AS FLOAT64) - CAST(amount0Out AS FLOAT64) > 0 THEN 1
ELSE -1 END AS trade_direction,
abs(CAST(amount0In AS FLOAT64) - CAST(amount0Out AS FLOAT64)) as amount0,
abs(CAST(amount1Out AS FLOAT64) - CAST(amount1In AS FLOAT64)) as amount1,
t.receipt_effective_gas_price,
t.receipt_gas_used,
t.receipt_effective_gas_price * (t.receipt_gas_used * POWER(10, -18)) as gas_cost_eth
FROM `blockchain-etl.ethereum_uniswap.UniswapV2Pair_event_Swap` s
LEFT JOIN `blockchain-etl.ethereum_uniswap.UniswapV2Factory_event_PairCreated` p ON s.contract_address = p.pair
INNER JOIN  `bigquery-public-data.crypto_ethereum.transactions` t ON s.transaction_hash = t.hash),

swaps_with_fees as (
SELECT *,
CASE WHEN trade_direction = 1 THEN 0.003 * amount0
ELSE 0.003 * amount1 end as fee_amount
FROM v2_swaps),

swaps_and_liq as (
SELECT s.*, reserve0 as asset0_liquidity, reserve1 as asset1_liquidity,
If(trade_direction = 1, reserve0, reserve1 ) as liquidity_in,
If(trade_direction = 1, reserve1, reserve0 ) as liquidity_out,
abs(If(trade_direction = 1, amount0, amount1)) as amount_in,
abs(If(trade_direction = 1, amount1, amount0)) as amount_out,
FROM swaps_with_fees s
INNER JOIN `blockchain-etl.ethereum_uniswap.UniswapV2Pair_event_Sync` ls
on ls.contract_address = s.contract_address and ls.transaction_hash = s.transaction_hash and ls.log_index = s.log_index -1),

swaps_with_implied_price as (
SELECT *,
      SAFE_DIVIDE((CAST(liquidity_out AS FLOAT64) + amount_out),  -- ratio of the liquidity _BEFORE_ the trade
                  (CAST(liquidity_in AS FLOAT64) - amount_in)) as implied_price,
FROM swaps_and_liq),

swaps_with_implied_amount_out as (
SELECT *,
implied_price * amount_in as implied_amount_out,
FROM swaps_with_implied_price)

SELECT *,
SAFE_DIVIDE(implied_amount_out - amount_out, implied_amount_out) as slippage_percentage,
(implied_price * amount_in - amount_out) as slippage,
FROM swaps_with_implied_amount_out