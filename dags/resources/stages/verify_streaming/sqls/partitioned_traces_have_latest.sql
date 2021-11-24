with top_partitioned_traces as (
  select block_timestamp
  from `{{params.internal_project_id}}.{{params.partitioned_dataset_name}}.traces_by_input_0xa90`
  where date(block_timestamp) >= date_add('{{ds}}', INTERVAL -1 DAY)
  union all
  select block_timestamp
  from `{{params.internal_project_id}}.{{params.partitioned_dataset_name}}.traces_by_input_0x70a`
  where date(block_timestamp) >= date_add('{{ds}}', INTERVAL -1 DAY)
  union all
  select block_timestamp
  from `{{params.internal_project_id}}.{{params.partitioned_dataset_name}}.traces_by_input_0x23b`
  where date(block_timestamp) >= date_add('{{ds}}', INTERVAL -1 DAY)
  union all
  select block_timestamp
  from `{{params.internal_project_id}}.{{params.partitioned_dataset_name}}.traces_by_input_0x090`
  where date(block_timestamp) >= date_add('{{ds}}', INTERVAL -1 DAY)
  union all
  select block_timestamp
  from `{{params.internal_project_id}}.{{params.partitioned_dataset_name}}.traces_by_input_0x022`
  where date(block_timestamp) >= date_add('{{ds}}', INTERVAL -1 DAY)
)
select if(
(
select timestamp_diff(
  current_timestamp(),
  (select max(block_timestamp)
  from top_partitioned_traces as traces
  where date(block_timestamp) >= date_add('{{ds}}', INTERVAL -1 DAY)),
  MINUTE)
) < {{params.max_lag_in_minutes}}, 1,
cast((select 'Partitioned traces are lagging by more than {{params.max_lag_in_minutes}} minutes') as INT64))
