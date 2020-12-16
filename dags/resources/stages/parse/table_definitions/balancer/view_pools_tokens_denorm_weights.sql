-- MIT License
-- Copyright (c) 2020 Balancer Labs, markus@balancer.finance
with events as (
    -- binds
    select block_number, transaction_index, 
    COALESCE(`blockchain-etl-internal.common.normalize_trace_address`(trace_address), '-----') as padded_trace_addres, 
    to_address as pool, token, cast(denorm as numeric) as denorm, 
    from `blockchain-etl.ethereum_balancer.BPool_call_bind`
    where status=1

    union all

    -- rebinds
    select block_number, transaction_index, 
    COALESCE(`blockchain-etl-internal.common.normalize_trace_address`(trace_address), '-----') as padded_trace_addres, 
    to_address as pool, token, cast(denorm as numeric) as denorm, 
    from `blockchain-etl.ethereum_balancer.BPool_call_rebind`
    where status=1

    union all
    
    -- unbinds
    select block_number, transaction_index, 
    COALESCE(`blockchain-etl-internal.common.normalize_trace_address`(trace_address), '-----') as padded_trace_addres, 
    to_address as pool, token, 0 as denorm, 
    from `blockchain-etl.ethereum_balancer.BPool_call_unbind`
    where status=1
),
last_event_in_transaction as (
    select block_number, transaction_index, pool, token, MAX(padded_trace_addres) as max_trace_address
    from events
    group by block_number, transaction_index, pool, token
),
last_transaction_in_block as (
    select events.block_number, events.pool, events.token, MAX(events.transaction_index) as max_transaction_index
    from events
    group by block_number, pool, token
),
state_with_gaps as (
    select events.block_number, events.pool, events.token, events.denorm,
    lead(events.block_number, 1, 99999999) over (partition by events.pool, events.token order by events.block_number) as next_block_number
    from events inner join last_transaction_in_block
    on events.block_number = last_transaction_in_block.block_number
    and events.pool = last_transaction_in_block.pool
    and events.token = last_transaction_in_block.token
    and events.transaction_index = last_transaction_in_block.max_transaction_index
    inner join last_event_in_transaction
    on events.block_number = last_event_in_transaction.block_number
    and events.pool = last_event_in_transaction.pool
    and events.token = last_event_in_transaction.token
    and events.transaction_index = last_event_in_transaction.transaction_index
    and events.padded_trace_addres = last_event_in_transaction.max_trace_address
),
calendar AS (
    select number as block_number from `bigquery-public-data.crypto_ethereum.blocks`
),
running_state as (
    select pool, token, calendar.block_number, denorm
    from state_with_gaps
    join calendar on state_with_gaps.block_number <= calendar.block_number and calendar.block_number < state_with_gaps.next_block_number
)
select pool as address, token as token_address, block_number, denorm
from running_state
