merge `{{params.destination_dataset_project_id}}.{{params.destination_dataset_name}}.{{params.table_name}}` dest
using {{params.dataset_name_temp}}.{{params.source_table}} source
on false
when not matched and date(block_timestamp) = '{{ds}}' then
insert (
    block_timestamp
    ,block_number
    ,transaction_hash

    {% if params.parser.type == 'log' %}
    ,log_index
    {% else %}
    ,trace_address
    {% endif %}

    {% for column in params.columns %}
    ,`{{ column }}`
    {% endfor %}
) values (
    block_timestamp
    ,block_number
    ,transaction_hash

    {% if params.parser.type == 'log' %}
    ,log_index
    {% else %}
    ,trace_address
    {% endif %}

    {% for column in params.columns %}
    ,`{{ column }}`
    {% endfor %}
)
when not matched by source and date(block_timestamp) = '{{ds}}' then
delete
