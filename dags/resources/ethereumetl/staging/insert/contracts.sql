INSERT INTO $CHAIN.contracts
SELECT
    staged_contracts.created_time AS created_time,
    staged_contracts.address AS address,
    staged_contracts.bytecode AS bytecode,
    staged_contracts.function_sighashes AS function_sighashes,
    staged_contracts.is_erc20 AS is_erc20,
    staged_contracts.is_erc721 AS is_erc721,
    staged_contracts.block_number AS block_number,
    staged_contracts.block_hash AS hash,
    staged_contracts.block_timestamp AS block_timestamp,
    staged_contracts.block_date AS block_date
FROM
    $CHAIN.contracts_staged_$EXECUTION_DATE_NODASH AS staged_contracts;