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
        sum(cast(receipt_gas_used as numeric) * cast((receipt_effective_gas_price - coalesce(base_fee_per_gas, 0)) as numeric)) as value
    from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.transactions` as transactions
    join `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.blocks` as blocks on blocks.number = transactions.block_number
    where true
    and date(transactions.block_timestamp) <= '{{ds}}'
    group by blocks.number, blocks.miner
    union all
    -- transaction fees credits
    select
        from_address as address,
        -(cast(receipt_gas_used as numeric) * cast(receipt_effective_gas_price as numeric)) as value
    from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.transactions`
    where true
    and date(block_timestamp) <= '{{ds}}'
    union all
    -- blob transaction fees credits
    select
        from_address as address,
        -(cast(receipt_blob_gas_used as numeric) * cast(receipt_blob_gas_price as numeric)) as value
    from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.transactions`
    where true
    and receipt_blob_gas_used > 0
    and date(block_timestamp) <= '{{ds}}'
    union all
    -- withdrawals
    select
        withdrawal.address as address,
        -- withdrawal amounts are in gwei, so multiplying by pow(10, 9)
        (cast(withdrawal.amount as NUMERIC) * cast(pow(10, 9) as NUMERIC)) as value
    from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.blocks`, unnest(withdrawals) as withdrawal
    where true
    and date(timestamp) <= '{{ds}}'
)
select address, sum(value) as eth_balance
from double_entry_book
group by address