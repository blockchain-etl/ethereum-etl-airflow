CREATE OR REPLACE TABLE `{destination_project_id}.{destination_dataset_name}.stage_root_call_traces_{ds_no_dashes}`
(
  trace_id           STRING,
  wallet_address     STRING    NOT NULL,
  contract_address   STRING,
  inactivity_minutes INT64,
  transaction_hash   STRING    NOT NULL,
  block_timestamp    TIMESTAMP NOT NULL,
  block_hash         STRING    NOT NULL,
  block_number       INT64     NOT NULL,
  value              NUMERIC,
  gas                INT64,
  gas_used           INT64,
  trace_type         STRING    NOT NULL,
  call_type          STRING,
  error              STRING,
  status             INT64
)
AS
SELECT
  traces.trace_id        AS trace_id,
  traces.from_address    AS wallet_address,
  traces.to_address      AS contract_address,

   DATETIME_DIFF(
    traces.block_timestamp,
    LAG(traces.block_timestamp) OVER (
      PARTITION BY traces.from_address, traces.to_address
      ORDER BY traces.block_timestamp ASC
    ),
    MINUTE
  )
                          AS inactivity_minutes,

  traces.transaction_hash AS transaction_hash,
  traces.block_timestamp  AS block_timestamp,
  traces.block_hash       AS block_hash,
  traces.block_number     AS block_number,

  value                   AS value,
  gas                     AS gas,
  gas_used                AS gas_used,
  trace_type              AS trace_type,
  call_type               AS call_type,
  error                   AS error,
  status                  AS status
FROM
  `{source_project_id}.{source_dataset_name}`.traces AS traces
WHERE
  -- Only use traces corresponding to the root of the call stack. See openethereum docs:
  -- https://openethereum.github.io/JSONRPC-trace-module
  trace_address IS NULL
  -- "call" traces log transfers of ether from one account to another and/or calls to a
  -- smart contract function defined by parameters in the data field. This trace also
  -- encompasses delegatecall and callcode.
  AND trace_type = 'call'
  -- Use traces overlapping the previous partition so we can extend existing sessions.
  AND block_timestamp >= timestamp_add(TIMESTAMP '{ds}', INTERVAL -30 MINUTE)
  AND block_timestamp < timestamp_add(TIMESTAMP '{ds}', INTERVAL 1 DAY)
ORDER BY
  wallet_address,
  block_timestamp ASC;
