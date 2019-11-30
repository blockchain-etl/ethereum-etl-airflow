select if(
(
select timestamp_diff(
  current_timestamp(),
  (select max(block_timestamp)
  from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.tokens` as tokens
  where date(block_timestamp) >= date_add('{{ds}}', INTERVAL -1 DAY)),
  MINUTE)
) < 600, 1,
cast((select 'Tokens are lagging by more than 600 minutes') as INT64))
