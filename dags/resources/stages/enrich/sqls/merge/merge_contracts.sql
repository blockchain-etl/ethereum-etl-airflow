merge `{{params.destination_dataset_project_id}}.{{params.destination_dataset_name}}.contracts` dest
using {{params.dataset_name_temp}}.{{params.source_table}} source
on false
when not matched and date(block_timestamp) = '{{ds}}' then
insert (
    address,
    bytecode,
    function_sighashes,
    is_erc20,
    is_erc721,
    block_timestamp,
    block_number,
    block_hash
) values (
    address,
    bytecode,
    function_sighashes,
    is_erc20,
    is_erc721,
    block_timestamp,
    block_number,
    block_hash
)
when not matched by source and date(block_timestamp) = '{{ds}}' then
delete
