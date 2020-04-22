merge `{{destination_dataset_project_id}}.{{destination_dataset_name}}.{{table_name}}` dest
using {{dataset_name_temp}}.{{source_table}} source
on false
when not matched and date(block_timestamp) = '{{ds}}' then
insert (
    block_timestamp
    ,block_number
    ,transaction_hash

    {% if parser.type == 'log' %}
    ,log_index
    ,contract_address
    {% else %}
    ,trace_address
    ,status
    ,error
    {% endif %}

    {% for column in table.schema %}
    ,`{{ column.name }}`
    {% endfor %}
) values (
    block_timestamp
    ,block_number
    ,transaction_hash

    {% if parser.type == 'log' %}
    ,log_index
    ,contract_address
    {% else %}
    ,trace_address
    ,status
    ,error
    {% endif %}

    {% for column in table.schema %}
    ,`{{ column.name }}`
    {% endfor %}
)
when not matched by source and date(block_timestamp) = '{{ds}}' then
delete
