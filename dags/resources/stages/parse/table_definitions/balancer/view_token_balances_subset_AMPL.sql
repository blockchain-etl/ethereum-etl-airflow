-- MIT License
-- Copyright (c) 2020 Balancer Labs, markus@balancer.finance

-- computing running balance from transfer events originally developed by Evgeny Medvedev
-- Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com (https://medium.com/google-cloud/plotting-ethereum-address-growth-chart-55cc0e7207b2)

-- the view below attempts to reconstruct the historical balance of AMPL tokens
-- at the end of each block, taking into account transfers and rebase events

-- Limitations: 
-- 1. values are approximate due to loss in numerical precision associated with casting values to FLOAT64 in SQL

-- DECLARE DECIMALS DEFAULT 9;
-- DECLARE INITIAL_FRAGMENTS_SUPPLY DEFAULT (50 * POW(10,6) * POW(10,DECIMALS));
-- DECLARE TOTAL_GONS DEFAULT cast('115792089237316195423570985008687907853269984665640564039457550000000000000000' as float64);

with events as (
  select block_number, log_index,
  cast('115792089237316195423570985008687907853269984665640564039457550000000000000000' as float64)/cast(totalSupply as float64) as gonsPerFragment 
  from `blockchain-etl.ethereum_ampleforth.UFragments_event_LogRebase`
  -- manually add a "ghost" event because initialization didn't emit a LogRebase event
  union all
  select 1, 1,  cast('115792089237316195423570985008687907853269984665640564039457550000000000000000' as float64)/(50 * POW(10,6) * POW(10,9))
),
gonsPerFragment_state_with_gaps as (
    select events.block_number, log_index,
    lead(events.block_number, 1, 99999999) over (order by events.block_number, events.log_index) as next_block_number,
    lead(events.log_index, 1, 99999999) over (order by events.block_number, events.log_index) as next_log_index,
    gonsPerFragment
    from events
),
fragments_double_entry_book as (
    -- debits
    select `to` as address, cast(value as FLOAT64) as fragment_value, block_number, log_index
    from `blockchain-etl.ethereum_ampleforth.UFragments_event_Transfer`
    where `to` <> '0x0000000000000000000000000000000000000000' 
    and date(block_timestamp) > "2019-06-01" 
    union all
    -- credits
    select `from` as address, -cast(value as FLOAT64) as fragment_value, block_number, log_index
  from `blockchain-etl.ethereum_ampleforth.UFragments_event_Transfer`
    where `from` <> '0x0000000000000000000000000000000000000000' 
    and date(block_timestamp) > "2019-06-01" 
), 
gons_double_entry_book as (
  select T.*, T.fragment_value * S.gonsPerFragment as value from fragments_double_entry_book T
  inner join gonsPerFragment_state_with_gaps S
  on (S.block_number < T.block_number and T.block_number < S.next_block_number)
  OR (S.block_number = T.block_number and S.next_block_number = T.block_number and S.log_index < T.log_index and T.log_index < S.next_log_index)
  OR (S.block_number = T.block_number and S.next_block_number > T.block_number and S.log_index < T.log_index)
  OR (S.next_block_number = T.block_number and S.block_number < T.block_number and S.next_log_index > T.log_index)
),
gons_double_entry_book_grouped_by_block as (
    select address, sum(value) as balance_increment, block_number
    from gons_double_entry_book
    group by address, block_number
),
blockly_gons_balances_with_gaps as (
    select address, block_number, sum(balance_increment) over (partition by address order by block_number) as balance,
    lead(block_number, 1, 99999999) over (partition by address order by block_number) as next_block_number
    from gons_double_entry_book_grouped_by_block
),
calendar AS (
    select number as block_number from `bigquery-public-data.crypto_ethereum.blocks`
),
running_gons_balances as (
    select address, calendar.block_number, balance
    from blockly_gons_balances_with_gaps a
    join calendar on a.block_number <= calendar.block_number and calendar.block_number < a.next_block_number
),
last_event_in_block as (
    select events.block_number, MAX(events.log_index) as max_log_index
    from events
    group by block_number
),
gonsPerFragment_state_at_end_of_block_with_gaps as (
    select events.block_number, events.gonsPerFragment,
    lead(events.block_number, 1, 99999999) over (order by events.block_number) as next_block_number
    from events inner join last_event_in_block
    on events.block_number = last_event_in_block.block_number
    and events.log_index = last_event_in_block.max_log_index
),
gonsPerFragment_running_state as (
    select calendar.block_number, gonsPerFragment
    from gonsPerFragment_state_at_end_of_block_with_gaps a
    join calendar on a.block_number <= calendar.block_number and calendar.block_number < a.next_block_number
),
running_fragments_balances as (
    select running_gons_balances.address, running_gons_balances.block_number,
    (balance / gonsPerFragment) as balance
    from running_gons_balances
    inner join gonsPerFragment_running_state
    on running_gons_balances.block_number = gonsPerFragment_running_state.block_number
)
select * from running_fragments_balances