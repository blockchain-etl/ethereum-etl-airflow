with double_entry_book as (
    -- debits
    select to_address as address, value as value
    from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.traces`
    where true
    and date(block_timestamp) <= '{{ds}}'
    and to_address is not null
    and status = 1
    and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
    union all
    -- credits
    select from_address as address, -value as value
    from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.traces`
    where true
    and date(block_timestamp) <= '{{ds}}'
    and from_address is not null
    and status = 1
    and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
    union all
    -- transaction fees debits
    select
        miner as address,
        if(blocks.number >= 12965000, -- EIP-1559, base_fee_per_gas * receipt_gas_used is burnt, remaining goes to miner
            sum(cast(receipt_gas_used as numeric) * cast((receipt_effective_gas_price - base_fee_per_gas) as numeric)),
            sum(cast(receipt_gas_used as numeric) * cast(gas_price as numeric))
        ) as value
    from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.transactions` as transactions
    join `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.blocks` as blocks on blocks.number = transactions.block_number
    where true
    and date(transactions.block_timestamp) <= '{{ds}}'
    group by blocks.number, blocks.miner
    union all
    -- transaction fees credits
    select
        from_address as address,
        if(block_number >= 12965000, -- EIP-1559, receipt_effective_gas_price is used instead of gas_price
           -(cast(receipt_gas_used as numeric) * cast(receipt_effective_gas_price as numeric)),
           -(cast(receipt_gas_used as numeric) * cast(gas_price as numeric))
        ) as value
    from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.transactions`
    where true
    and date(block_timestamp) <= '{{ds}}'
)
select address, sum(value) as eth_balance
from double_entry_book
group by address