merge `{{internal_project_id}}.{{dataset_name}}.{{destination_table_name}}` dest
using {{dataset_name_temp}}.{{source_table}} source_table
on false
when not matched and date(block_timestamp) = '{{ds}}' then
insert (
    {% for column in table_schema %}
    {% if loop.index0 > 0 %},{% endif %}`{{ column.name }}`
    {% endfor %}
) values (
    {% for column in table_schema %}
    {% if loop.index0 > 0 %},{% endif %}`{{ column.name }}`
    {% endfor %}
)
when not matched by source and date(block_timestamp) = '{{ds}}' then
delete
