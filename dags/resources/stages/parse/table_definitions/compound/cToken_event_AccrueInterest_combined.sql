-- AccrueInterest for some tokens don't include the cashPrior field. This view combines events with both signatures
SELECT block_timestamp, block_number, transaction_hash, log_index, contract_address, cashPrior, interestAccumulated, borrowIndex, totalBorrows
FROM `blockchain-etl.ethereum_compound.cToken_event_AccrueInterest` 
UNION ALL
SELECT block_timestamp, block_number, transaction_hash, log_index, contract_address, null, interestAccumulated, borrowIndex, totalBorrows
FROM `blockchain-etl.ethereum_compound.cUSDC_event_AccrueInterest`
UNION ALL
SELECT block_timestamp, block_number, transaction_hash, log_index, contract_address, null, interestAccumulated, borrowIndex, totalBorrows
FROM `blockchain-etl.ethereum_compound.cREP_event_AccrueInterest`
UNION ALL
SELECT block_timestamp, block_number, transaction_hash, log_index, contract_address, null, interestAccumulated, borrowIndex, totalBorrows
FROM `blockchain-etl.ethereum_compound.cBAT_event_AccrueInterest`
UNION ALL
SELECT block_timestamp, block_number, transaction_hash, log_index, contract_address, null, interestAccumulated, borrowIndex, totalBorrows
FROM `blockchain-etl.ethereum_compound.cETH_event_AccrueInterest`
UNION ALL
SELECT block_timestamp, block_number, transaction_hash, log_index, contract_address, null, interestAccumulated, borrowIndex, totalBorrows
FROM `blockchain-etl.ethereum_compound.cSAI_event_AccrueInterest`
UNION ALL
SELECT block_timestamp, block_number, transaction_hash, log_index, contract_address, null, interestAccumulated, borrowIndex, totalBorrows
FROM `blockchain-etl.ethereum_compound.cWBTC_event_AccrueInterest`
UNION ALL
SELECT block_timestamp, block_number, transaction_hash, log_index, contract_address, null, interestAccumulated, borrowIndex, totalBorrows
FROM `blockchain-etl.ethereum_compound.cZRX_event_AccrueInterest`