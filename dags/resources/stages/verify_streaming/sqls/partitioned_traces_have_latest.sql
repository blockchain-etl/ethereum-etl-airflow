select if(
(
select timestamp_diff(
  current_timestamp(),
  (select max(block_timestamp)
  from `{{params.internal_project_id}}.{{params.partitioned_dataset_name}}.traces_by_to_address_0x7f6` as traces
  where date(block_timestamp) >= date_add('{{ds}}', INTERVAL -1 DAY)),
  MINUTE)
) < {{params.max_lag_in_minutes}}, 1,
cast((select 'Partitioned traces are lagging by more than {{params.max_lag_in_minutes}} minutes') as INT64))
