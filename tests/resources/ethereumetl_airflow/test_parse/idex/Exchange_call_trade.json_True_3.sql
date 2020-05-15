select *
from `blockchain-etl-internal.ethereum_idex.Exchange_call_trade_history`
where date(block_timestamp) <= '2020-01-01'
union all
select *
from `blockchain-etl-internal.ethereum_idex.Exchange_call_trade`
where date(block_timestamp) > '2020-01-01'