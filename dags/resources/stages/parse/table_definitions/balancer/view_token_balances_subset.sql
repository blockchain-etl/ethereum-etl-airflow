-- MIT License
-- Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com (https://medium.com/google-cloud/plotting-ethereum-address-growth-chart-55cc0e7207b2)
-- Copyright (c) 2020 Balancer Labs, markus@balancer.finance

-- Block by block balance of ERC20 tokens per token holder (based on transfers occurring after 2020-March-01)
-- This view supports querying BPT token holders ("shareholders" of pools) and the balance of tokens in pools

-- Limitation: tokens must emit Transfer event every time the balance of the token holder is updated
-- eg: Balancer Pool Tokens emit the Transfer event when minted
-- eg: WETH does not emit the Transfer event when minted, but the 
-- scope of this view is *pools' balances* of WETH, and pools don't mint WETH

with double_entry_book as (
    -- debits
    select token_address, to_address as address, cast(value as FLOAT64) as value, block_number
    from `bigquery-public-data.crypto_ethereum.token_transfers`
    where to_address <> '0x0000000000000000000000000000000000000000' 
    and date(block_timestamp) > "2020-03-01" 
    union all
    -- credits
    select token_address, from_address as address, -cast(value as FLOAT64) as value, block_number
    from `bigquery-public-data.crypto_ethereum.token_transfers`
    where from_address <> '0x0000000000000000000000000000000000000000' 
    and date(block_timestamp) > "2020-03-01" 
),
double_entry_book_grouped_by_block as (
    select token_address, address, sum(value) as balance_increment, block_number
    from double_entry_book
    group by token_address, address, block_number
),
blockly_balances_with_gaps as (
    select token_address, address, block_number, sum(balance_increment) over (partition by token_address, address order by block_number) as balance,
    lead(block_number, 1, 99999999) over (partition by token_address, address order by block_number) as next_block_number
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
-- remove AMPL because of rebasing. use view_token_balances_subset_AMPL
where token_address not in ('0xd46ba6d942050d489dbd938a2c909a5d5039a161')