select if(
(
select timestamp_diff(
  current_timestamp(),
  (select max(block_timestamp)
  from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.transactions` as transactions
  where date(block_timestamp) >= date_add('{{ds}}', INTERVAL -1 DAY)),
  MINUTE)
) < {{params.max_lag_in_minutes}}, 1,
cast((select 'Transactions are lagging by more than {{params.max_lag_in_minutes}} minutes') as INT64))
