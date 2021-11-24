-- MIT License
-- Copyright (c) 2020 Balancer Labs, markus@balancer.finance

-- computing running balance from transfer events originally developed by Evgeny Medvedev
-- Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com (https://medium.com/google-cloud/plotting-ethereum-address-growth-chart-55cc0e7207b2)

-- the view below attempts to reconstruct the historical balance of MARK tokens
-- at the end of each block, taking into account transfers and rebase events

-- Limitations: 
-- 1. values are approximate due to loss in numerical precision associated with casting values to FLOAT64 in SQL

--         decimals = 9;                               // decimals  
--         totalSupply = 75000000*10**9;                // initialSupply
--         totalAtoms = MAX_UINT256 - (MAX_UINT256 % totalSupply);     // totalAtoms is a multiple of totalSupply so that atomsPerMolecule is an integer.


with events as (
  select block_number, log_index,
  cast('115792089237316195423570985008687907853269984665640564039457525000000000000000' as float64)/cast(_totalSupply as float64) as atomsPerMolecule 
  from `blockchain-etl.ethereum_benchmark.Benchmark_event_LogRebase`
  -- manually add a "ghost" event because initialization didn't emit a LogRebase event
  union all
  select 1, 1,  cast('115792089237316195423570985008687907853269984665640564039457525000000000000000' as float64)/(75000000 * POW(10,9))
),
atomsPerMolecule_state_with_gaps as (
    select events.block_number, log_index,
    lead(events.block_number, 1, 99999999) over (order by events.block_number, events.log_index) as next_block_number,
    lead(events.log_index, 1, 99999999) over (order by events.block_number, events.log_index) as next_log_index,
    atomsPerMolecule
    from events
),
molecules_double_entry_book as (
    -- debits
    select `_to` as address, cast(_value as FLOAT64) as molecule_value, block_number, log_index
    from `blockchain-etl.ethereum_benchmark.Benchmark_event_Transfer`
    where `_to` <> '0x0000000000000000000000000000000000000000' 
    and date(block_timestamp) > "2020-11-15" 
    union all
    -- credits
    select `_from` as address, -cast(_value as FLOAT64) as molecule_value, block_number, log_index
  from `blockchain-etl.ethereum_benchmark.Benchmark_event_Transfer`
    where `_from` <> '0x0000000000000000000000000000000000000000' 
    and date(block_timestamp) > "2020-11-15" 
), 
atoms_double_entry_book as (
  select T.*, T.molecule_value * S.atomsPerMolecule as value from molecules_double_entry_book T
  inner join atomsPerMolecule_state_with_gaps S
  on (S.block_number < T.block_number and T.block_number < S.next_block_number)
  OR (S.block_number = T.block_number and S.next_block_number = T.block_number and S.log_index < T.log_index and T.log_index < S.next_log_index)
  OR (S.block_number = T.block_number and S.next_block_number > T.block_number and S.log_index < T.log_index)
  OR (S.next_block_number = T.block_number and S.block_number < T.block_number and S.next_log_index > T.log_index)
),
atoms_double_entry_book_grouped_by_block as (
    select address, sum(value) as balance_increment, block_number
    from atoms_double_entry_book
    group by address, block_number
),
blockly_atoms_balances_with_gaps as (
    select address, block_number, sum(balance_increment) over (partition by address order by block_number) as balance,
    lead(block_number, 1, 99999999) over (partition by address order by block_number) as next_block_number
    from atoms_double_entry_book_grouped_by_block
),
calendar AS (
    select number as block_number from `bigquery-public-data.crypto_ethereum.blocks`
),
running_atoms_balances as (
    select address, calendar.block_number, balance
    from blockly_atoms_balances_with_gaps a
    join calendar on a.block_number <= calendar.block_number and calendar.block_number < a.next_block_number
),
last_event_in_block as (
    select events.block_number, MAX(events.log_index) as max_log_index
    from events
    group by block_number
),
atomsPerMolecule_state_at_end_of_block_with_gaps as (
    select events.block_number, events.atomsPerMolecule,
    lead(events.block_number, 1, 99999999) over (order by events.block_number) as next_block_number
    from events inner join last_event_in_block
    on events.block_number = last_event_in_block.block_number
    and events.log_index = last_event_in_block.max_log_index
),
atomsPerMolecule_running_state as (
    select calendar.block_number, atomsPerMolecule
    from atomsPerMolecule_state_at_end_of_block_with_gaps a
    join calendar on a.block_number <= calendar.block_number and calendar.block_number < a.next_block_number
),
running_molecules_balances as (
    select running_atoms_balances.address, running_atoms_balances.block_number,
    (balance / atomsPerMolecule) as balance
    from running_atoms_balances
    inner join atomsPerMolecule_running_state
    on running_atoms_balances.block_number = atomsPerMolecule_running_state.block_number
)
select * from running_molecules_balances