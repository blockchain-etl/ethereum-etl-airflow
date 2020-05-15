merge `blockchain-etl-internal.ethereum_ens.Registrar0_event_NewBid_history` dest
using parse_temp.temp_Registrar0_event_NewBid_1587556654993 source
on false
when not matched and date(block_timestamp) = '2020-01-01' then
insert (

    `block_timestamp`

    ,`block_number`

    ,`transaction_hash`

    ,`log_index`

    ,`contract_address`

    ,`hash`

    ,`bidder`

    ,`deposit`

) values (

    `block_timestamp`

    ,`block_number`

    ,`transaction_hash`

    ,`log_index`

    ,`contract_address`

    ,`hash`

    ,`bidder`

    ,`deposit`

)
when not matched by source and date(block_timestamp) = '2020-01-01' then
delete