CREATE VIEW $CHAIN.contracts_staged_$EXECUTION_DATE_NODASH AS
    SELECT
    toUnixTimestamp(now())  AS created_time,
    staged_contracts.address AS address,
    staged_contracts.bytecode AS bytecode,
    staged_contracts.function_sighashes AS function_sighashes,
    staged_contracts.is_erc20 AS is_erc20,
    staged_contracts.is_erc721 AS is_erc721,
    staged_contracts.block_number AS block_number,
    blocks_master.hash AS block_hash,
    blocks_master.timestamp AS block_timestamp,
    toDate(blocks_master.timestamp) AS block_date
FROM
    $CHAIN.contracts_$EXECUTION_DATE_NODASH AS staged_contracts
LEFT JOIN
    $CHAIN.blocks AS blocks_master ON
    (staged_contracts.block_number = blocks_master.number);