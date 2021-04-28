-- MIT License
-- Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com (https://medium.com/google-cloud/plotting-ethereum-address-growth-chart-55cc0e7207b2)
-- Copyright (c) 2020 Balancer Labs, markus@balancer.finance

-- Block by block balance of ERC20 tokens held per Balancer V2 pool id
-- Balance = Total Balance = Cash + Managed

with double_entry_book as (
    SELECT 
      block_number,
      poolId, 
      LOWER(tokens) AS token_address, 
      CAST(deltas as FLOAT64) - CAST(protocolFeeAmounts as FLOAT64) AS total_change,
      CAST(deltas as FLOAT64) - CAST(protocolFeeAmounts as FLOAT64) AS cash_change,
      0 AS managed_change
    FROM `blockchain-etl.ethereum_balancer.V2_Vault_event_PoolBalanceChanged`
    CROSS JOIN UNNEST(SPLIT(tokens,',')) AS tokens WITH OFFSET AS offset1
    CROSS JOIN UNNEST(SPLIT(deltas,',')) AS deltas WITH OFFSET AS offset2
    CROSS JOIN UNNEST(SPLIT(protocolFeeAmounts,',')) AS protocolFeeAmounts WITH OFFSET AS offset3
    ON offset1 = offset2 AND offset1 = offset3

    UNION ALL

    SELECT 
      block_number,
      poolId, 
      tokenIn AS token_address, 
      CAST(amountIn as FLOAT64) AS total_change,
      CAST(amountIn as FLOAT64) AS cash_change,
      0 AS managed_change
    FROM `blockchain-etl.ethereum_balancer.V2_Vault_event_Swap`

    UNION ALL

    SELECT 
      block_number,
      poolId, 
      tokenOut AS token_address, 
      -CAST(amountOut as FLOAT64) AS total_change,
      -CAST(amountOut as FLOAT64) AS cash_change,
      0 AS managed_change
    FROM `blockchain-etl.ethereum_balancer.V2_Vault_event_Swap`

    UNION ALL

    SELECT 
      block_number,
      poolId, 
      token as token_address, 
      CAST(cashDelta as FLOAT64) + CAST(managedDelta as FLOAT64) AS total_change,
      CAST(cashDelta as FLOAT64) AS cash_change,
      CAST(managedDelta as FLOAT64) AS managed_change,
    FROM `blockchain-etl.ethereum_balancer.V2_Vault_event_PoolBalanceManaged`
),
double_entry_book_grouped_by_block as (
    SELECT 
      block_number,
      poolId,
      token_address,
      SUM(total_change) as total_change,
      SUM(cash_change) as cash_change,
      SUM(managed_change) as managed_change
    from double_entry_book
    group by token_address, poolId, block_number
),
blockly_balances_with_gaps as (
    SELECT 
      block_number,
      poolId,
      token_address,
      SUM(total_change) OVER (PARTITION BY token_address, poolId ORDER BY block_number) AS total_balance,
      SUM(cash_change) OVER (PARTITION BY token_address, poolId ORDER BY block_number) AS cash_balance,
      SUM(managed_change) OVER (PARTITION BY token_address, poolId ORDER BY block_number) AS managed_balance,
      LEAD(block_number, 1, 99999999) OVER (PARTITION BY token_address, poolId ORDER BY block_number) AS next_block_number
    FROM double_entry_book_grouped_by_block
),
calendar AS (
    select number as block_number from `bigquery-public-data.crypto_ethereum.blocks`
),
running_balances as (
    select token_address, poolId, calendar.block_number, total_balance, cash_balance, managed_balance
    from blockly_balances_with_gaps
    join calendar on blockly_balances_with_gaps.block_number <= calendar.block_number and calendar.block_number < blockly_balances_with_gaps.next_block_number
)
SELECT 
  block_number,
  poolId,
  token_address,
  total_balance, 
  cash_balance, 
  managed_balance
from running_balances