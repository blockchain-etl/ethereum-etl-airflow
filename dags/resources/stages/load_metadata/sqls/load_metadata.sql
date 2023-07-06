SELECT
    '{{ params.chain }}' AS chain,
    CAST('{{ params.load_all_partitions }}' AS BOOL) AS load_all_partitions,
    CAST('{{ ds }}' AS DATE) AS ds,
    '{{ run_id }}' AS run_id,
    TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), SECOND) AS complete_at
