select if(
(
select max(number) from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.blocks`
where date(timestamp) <= '{{ds}}'
) + 1 =
(
select count(*) from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.blocks`
where date(timestamp) <= '{{ds}}'
), 1,
cast((select 'Total number of blocks except genesis is not equal to last block number {{ds}}') as int64))
