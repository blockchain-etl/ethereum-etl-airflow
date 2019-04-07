SELECT
    logs.log_index,
    logs.transaction_hash,
    logs.transaction_index,
    logs.address,
    logs.data,
    logs.topics,
    TIMESTAMP_SECONDS(blocks.timestamp) AS block_timestamp,
    blocks.number AS block_number,
    blocks.hash AS block_hash
FROM {{params.dataset_name_raw}}.blocks AS blocks
    JOIN {{params.dataset_name_raw}}.logs AS logs ON blocks.number = logs.block_number
where true
    {% if not params.load_all_partitions %}
    and date(timestamp_seconds(blocks.timestamp)) = '{{ds}}'
    {% endif %}