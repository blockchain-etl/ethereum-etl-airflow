SELECT
    contracts.address,
    contracts.bytecode,
    contracts.function_sighashes,
    contracts.is_erc20,
    contracts.is_erc721,
    TIMESTAMP_SECONDS(blocks.timestamp) AS block_timestamp,
    blocks.number AS block_number,
    blocks.hash AS block_hash
FROM blockchain_raw.contracts AS contracts
    JOIN blockchain_raw.receipts AS receipts ON receipts.contract_address = contracts.address
    JOIN blockchain_raw.blocks AS blocks ON blocks.number = receipts.block_number
