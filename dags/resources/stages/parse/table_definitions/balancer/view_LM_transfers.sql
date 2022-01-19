-- MIT License
-- Copyright (c) 2020 Balancer Labs, markus@balancer.finance

-- Transfers of Liquidity Mining Power
-- On Mainnet, Liquidity Mining Power is derived from V2 BPTs plus:
--   1. Aave's staking module
--   2. GRO's staking contract

-- V1 stkABPT transfers
select 
  contract_address as token_address,
  `from` as from_address,
  `to` as to_address,
  value,
  block_number,
  block_timestamp
FROM `blockchain-etl.ethereum_balancer.V1_stkABPT_event_Transfer` 
union all
-- V2 BPT transfers
select 
  contract_address as token_address,
  `from` as from_address,
  `to` as to_address,
  value,
  block_number,
  block_timestamp
FROM `blockchain-etl.ethereum_balancer.V2_BalancerPoolToken_event_Transfer` 
union all
-- 80/20 GRO pool staking contract deposits
-- are handled as trasnfers back to the user
-- so that the token transfer and the deposit cancel each other out
select
  '0x702605f43471183158938c1a3e5f5a359d7b31ba' as token_address,
  '0x001c249c09090d79dc350a286247479f08c7aad7' as from_address,
  user as to_address,
  amount as value,
  block_number,
  block_timestamp
FROM `blockchain-etl.ethereum_gro.LPTokenStaker_event_LogDeposit` 
WHERE pid = '5'
union all
-- 80/20 GRO pool staking contract withdrawals
-- are handled as transfers back to the staking contract
-- so that the token transfer and the withdraw cancel each other out
select
  '0x702605f43471183158938c1a3e5f5a359d7b31ba' as token_address,
  user as from_address,
  '0x001c249c09090d79dc350a286247479f08c7aad7' as to_address,
  amount as value,
  block_number,
  block_timestamp
FROM `blockchain-etl.ethereum_gro.LPTokenStaker_event_LogWithdraw` 
WHERE pid = '5'