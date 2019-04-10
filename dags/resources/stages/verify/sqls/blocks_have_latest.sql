select if(
(
select count(*) from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.blocks`
where date(timestamp) = '{{ds}}'
) > 0, 1,
cast((select 'There are no blocks on {{ds}}') as int64))
