select *
from `blockchain-etl.ethereum_ens_internal.Registrar0_event_NewBid_history`
where date(block_timestamp) <= '2020-01-01'
union all
select *
from `blockchain-etl.ethereum_ens_internal.Registrar0_event_NewBid`
where date(block_timestamp) > '2020-01-01'