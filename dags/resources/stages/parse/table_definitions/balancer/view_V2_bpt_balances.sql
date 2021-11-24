-- MIT License
-- Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com (https://medium.com/google-cloud/plotting-ethereum-address-growth-chart-55cc0e7207b2)
-- Copyright (c) 2020 Balancer Labs, markus@balancer.finance

-- Block by block balance of BPT tokens
-- This view supports querying BPT token holders ("shareholders" of pools)

with bpt_transfers as (
    select 
      contract_address as token_address,
      `from` as from_address,
      `to` as to_address,
      value,
      block_number
    FROM `blockchain-etl.ethereum_balancer.V2_WeightedPool_event_Transfer` 
    union all
    select 
      contract_address as token_address,
      `from` as from_address,
      `to` as to_address,
      value,
      block_number
    FROM `blockchain-etl.ethereum_balancer.V2_WeightedPool2Tokens_event_Transfer`
    union all
    select 
      contract_address as token_address,
      `from` as from_address,
      `to` as to_address,
      value,
      block_number
    FROM `blockchain-etl.ethereum_balancer.V2_StablePool_event_Transfer`
    union all
    select 
      contract_address as token_address,
      `from` as from_address,
      `to` as to_address,
      value,
      block_number
    FROM `blockchain-etl.ethereum_balancer.V2_MetaStablePool_event_Transfer`
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