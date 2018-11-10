with enriched_traces as (
    -- Find all nested traces of failed traces
    with nested_failed_traces as (
        select distinct child.transaction_hash, child.trace_address
        from ethereum_blockchain.traces parent
        join ethereum_blockchain.traces child
        on (parent.trace_address is null or starts_with(child.trace_address, concat(parent.trace_address, ',')))
        and child.transaction_hash = parent.transaction_hash
        where parent.trace_type in ('call', 'create')
        and parent.error is not null
    )
    -- is_error also accounts for whether the parent trace failed
    select traces.*, (traces.error is not null or nested_failed_traces.trace_address is not null) as is_error
    from `ethereum_blockchain.traces` as traces
    left join nested_failed_traces on nested_failed_traces.transaction_hash = traces.transaction_hash
    and nested_failed_traces.trace_address = traces.trace_address
),
double_entry_book as (
    -- debits
    select to_address as address, value as value, block_timestamp
    from enriched_traces
    where to_address is not null
    and not is_error
    and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
    union all
    -- credits
    select from_address as address, -value as value, block_timestamp
    from enriched_traces
    where from_address is not null
    and not is_error
    and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
    union all
    -- transaction fees debits
    select miner as address, sum(cast(receipt_gas_used as numeric) * cast(gas_price as numeric)) as value, blocks.timestamp
    from `bigquery-public-data.ethereum_blockchain.transactions` as transactions
    join `bigquery-public-data.ethereum_blockchain.blocks` as blocks on blocks.number = transactions.block_number
    group by blocks.timestamp, blocks.miner
    union all
    -- transaction fees credits
    select from_address as address, -(cast(receipt_gas_used as numeric) * cast(gas_price as numeric)) as value, block_timestamp
    from `bigquery-public-data.ethereum_blockchain.transactions`
),
balances as (
  select address, sum(value) as balance
  from double_entry_book
  group by address
)
select address, balance
from balances
where balance > 0
order by balance desc
limit 10
