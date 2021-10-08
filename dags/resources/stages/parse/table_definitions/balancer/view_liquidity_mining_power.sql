-- MIT License
-- Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com (https://medium.com/google-cloud/plotting-ethereum-address-growth-chart-55cc0e7207b2)
-- Copyright (c) 2020 Balancer Labs, markus@balancer.finance

-- Block by block balance of "mining power" for the purposes of liquidity mining
-- This view supports querying V2 BPT token holders ("shareholders" of pools)
-- as well as some special cases, namely:
--   1. Aave's staking module
--   2. GRO's staking contract

with bpt_transfers as (
    -- V1 stkABPT transfers
    select 
      contract_address as token_address,
      `from` as from_address,
      `to` as to_address,
      value,
      block_number
    FROM `blockchain-etl.ethereum_balancer.V1_stkABPT_event_Transfer` 
    union all
    -- V2 BPT transfers
    select 
      contract_address as token_address,
      `from` as from_address,
      `to` as to_address,
      value,
      block_number
    FROM `blockchain-etl.ethereum_balancer.V2_BalancerPoolToken_event_Transfer` 
    union all
    -- 80/20 GRO pool staking contract deposits
    -- are handled as trasnfers back to the user
    -- so that the token transfer and the deposit cancel each other out
    select
      '0x702605f43471183158938c1a3e5f5a359d7b31ba' as token_address,
      '0x001c249c09090d79dc350a286247479f08c7aad7' as from_address,
      user as to_address,
      amount as value,
      block_number
    FROM `blockchain-etl.ethereum_gro.LPTokenStaker_event_LogDeposit` 
    WHERE pid = '5'
    union all
    -- 80/20 GRO pool staking contract withdrawals
    -- are handled as transfers back to the staking contract
    -- so that the token transfer and the withdraw cancel each other out
    select
      '0x702605f43471183158938c1a3e5f5a359d7b31ba' as token_address,
      user as from_address,
      '0x001c249c09090d79dc350a286247479f08c7aad7' as to_address,
      amount as value,
      block_number
    FROM `blockchain-etl.ethereum_gro.LPTokenStaker_event_LogWithdraw` 
    WHERE pid = '5'
),
double_entry_book as (
    -- debits
    select token_address, to_address as address, cast(value as FLOAT64) as value, block_number
    from bpt_transfers
    where to_address <> '0x0000000000000000000000000000000000000000' 
    union all
    -- credits
    select token_address, from_address as address, -cast(value as FLOAT64) as value, block_number
    from bpt_transfers
    where from_address <> '0x0000000000000000000000000000000000000000' 
),
double_entry_book_grouped_by_block as (
    select token_address, address, sum(value) as balance_increment, block_number
    from double_entry_book
    group by token_address, address, block_number
),
blockly_balances_with_gaps as (
    select token_address, address, block_number, sum(balance_increment) over (partition by token_address, address order by block_number) as balance,
    lead(block_number, 1, 999999999999999999) over (partition by token_address, address order by block_number) as next_block_number
    from double_entry_book_grouped_by_block
),
calendar AS (
    select number as block_number from `bigquery-public-data.crypto_ethereum.blocks`
),
running_balances as (
    select token_address, address, calendar.block_number, balance
    from blockly_balances_with_gaps
    join calendar on blockly_balances_with_gaps.block_number <= calendar.block_number and calendar.block_number < blockly_balances_with_gaps.next_block_number
)
select token_address, address, block_number, balance
from running_balances