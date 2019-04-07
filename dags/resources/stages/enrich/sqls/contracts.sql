SELECT
    contracts.address,
    contracts.bytecode,
    contracts.function_sighashes,
    contracts.is_erc20,
    contracts.is_erc721,
    TIMESTAMP_SECONDS(blocks.timestamp) AS block_timestamp,
    blocks.number AS block_number,
    blocks.hash AS block_hash
FROM {{params.dataset_name_raw}}.contracts AS contracts
    JOIN {{params.dataset_name_raw}}.blocks AS blocks ON contracts.block_number = blocks.number
where true
    {% if not params.load_all_partitions %}
    and date(timestamp_seconds(blocks.timestamp)) = '{{ds}}'
    {% endif %}

