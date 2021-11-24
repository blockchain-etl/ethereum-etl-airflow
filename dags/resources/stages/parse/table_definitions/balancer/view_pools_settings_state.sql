-- MIT License
-- Copyright (c) 2020 Balancer Labs, markus@balancer.finance
select COALESCE(C.address, COALESCE(F.address, S.address)) as address, 
COALESCE(C.block_number, COALESCE(F.block_number, S.block_number)) as block_number, 
COALESCE(public_,'false') as public_swap,
COALESCE(swapFee,'1000000000000') as swapfee, 
controller
FROM `blockchain-etl.ethereum_balancer.view_pools_controllers` C
FULL JOIN `blockchain-etl.ethereum_balancer.view_pools_fees` F
ON C.address = F.address AND C.block_number = F.block_number
FULL JOIN `blockchain-etl.ethereum_balancer.view_pools_public_swaps` S
ON C.address = S.address AND C.block_number = S.block_number