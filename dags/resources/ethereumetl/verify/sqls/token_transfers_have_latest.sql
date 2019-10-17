select if(
(
select count(*) from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.token_transfers`
where date(block_timestamp) = '{{ds}}'
) > 0, 1,
cast((select 'There are no token transfers on {{ds}}') as int64))
