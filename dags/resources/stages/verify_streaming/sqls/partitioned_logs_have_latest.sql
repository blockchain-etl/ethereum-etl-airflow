select if(
(
select timestamp_diff(
  current_timestamp(),
  (select max(block_timestamp)
  from `{{params.internal_project_id}}.{{params.partitioned_dataset_name}}.logs_by_topic_0xddf` as logs
  where date(block_timestamp) >= date_add('{{ds}}', INTERVAL -1 DAY)),
  MINUTE)
) < {{params.max_lag_in_minutes}}, 1,
cast((select 'Partitioned logs are lagging by more than {{params.max_lag_in_minutes}} minutes') as INT64))
