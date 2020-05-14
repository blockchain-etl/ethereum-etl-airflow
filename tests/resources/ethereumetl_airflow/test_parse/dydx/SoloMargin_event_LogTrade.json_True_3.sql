select *
from `blockchain-etl.ethereum_dydx_internal.SoloMargin_event_LogTrade_history`
where date(block_timestamp) <= '2020-01-01'
union all
select *
from `blockchain-etl.ethereum_dydx_internal.SoloMargin_event_LogTrade`
where date(block_timestamp) > '2020-01-01'