select *
from `blockchain-etl.ethereum_uniswap_internal.Uniswap_event_AddLiquidity_history`
where date(block_timestamp) <= '2020-01-01'
union all
select *
from `blockchain-etl.ethereum_uniswap_internal.Uniswap_event_AddLiquidity`
where date(block_timestamp) > '2020-01-01'