SELECT 
  poolId, 
  LOWER(tokens) AS token_address, 
  CAST(weights as FLOAT64)/1e18 AS normalized_weight
from `blockchain-etl.ethereum_balancer.V2_Vault_event_PoolRegistered` r
inner join `blockchain-etl.ethereum_balancer.V2_WeightedPoolFactory_call_create` c
on c.transaction_hash = r.transaction_hash
CROSS JOIN UNNEST(SPLIT(tokens,',')) AS tokens WITH OFFSET AS offset1
CROSS JOIN UNNEST(SPLIT(weights,',')) AS weights WITH OFFSET AS offset2
ON offset1 = offset2

UNION ALL

SELECT 
  poolId, 
  LOWER(tokens) AS token_address, 
  CAST(weights as FLOAT64)/1e18 AS normalized_weight
from `blockchain-etl.ethereum_balancer.V2_Vault_event_PoolRegistered` r
inner join `blockchain-etl.ethereum_balancer.V2_WeightedPool2TokensFactory_call_create` c
on c.transaction_hash = r.transaction_hash
CROSS JOIN UNNEST(SPLIT(tokens,',')) AS tokens WITH OFFSET AS offset1
CROSS JOIN UNNEST(SPLIT(weights,',')) AS weights WITH OFFSET AS offset2
ON offset1 = offset2