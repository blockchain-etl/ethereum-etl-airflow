merge `blockchain-etl-internal.ethereum_uniswap.UniswapV2Pair_event_Swap_history` dest
using parse_temp.temp_UniswapV2Pair_event_Swap_1587556654993 source_table
on false
when not matched and date(block_timestamp) = '2020-01-01' then
insert (
    
    `block_timestamp`
    
    ,`block_number`
    
    ,`transaction_hash`
    
    ,`log_index`
    
    ,`contract_address`
    
    ,`sender`
    
    ,`amount0In`
    
    ,`amount1In`
    
    ,`amount0Out`
    
    ,`amount1Out`
    
    ,`to`
    
) values (
    
    `block_timestamp`
    
    ,`block_number`
    
    ,`transaction_hash`
    
    ,`log_index`
    
    ,`contract_address`
    
    ,`sender`
    
    ,`amount0In`
    
    ,`amount1In`
    
    ,`amount0Out`
    
    ,`amount1Out`
    
    ,`to`
    
)
when not matched by source and date(block_timestamp) = '2020-01-01' then
delete