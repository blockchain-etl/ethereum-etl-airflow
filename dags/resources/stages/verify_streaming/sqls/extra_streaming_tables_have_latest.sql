select if(
(
select timestamp_diff(
  current_timestamp(),
  (select max(timestamp_seconds(cast(json_extract(data, '$.timestamp') AS INTEGER)))
  from `{{params.streaming_table}}`),
  MINUTE)
) < {{params.max_lag_in_minutes}}, 1,
cast((select 'Streaming table {{params.streaming_table}} is lagging by more than {{params.max_lag_in_minutes}} minutes') as INT64))
