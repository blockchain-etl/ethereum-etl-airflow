-- Create a staging table.
CREATE TEMPORARY TABLE `stage_sessions_{ds_no_dashes}`
(
  id                                 STRING    NOT NULL,
  start_block_number                 INT64     NOT NULL,
  start_block_timestamp              TIMESTAMP NOT NULL,
  wallet_address                     STRING    NOT NULL,
  contract_address                   STRING
);


-- Stage sessions for this execution date.
INSERT INTO `stage_sessions_{ds_no_dashes}`
SELECT
  TO_HEX(MD5(CONCAT(
    trace_id,
    wallet_address,
    contract_address,
    block_timestamp
  )))
                    AS id,

  block_number      AS start_block_number,
  block_timestamp   AS start_block_timestamp,
  wallet_address    AS wallet_address,
  contract_address  AS contract_address
FROM
  `{destination_project_id}.{temp_dataset_name}.stage_root_call_traces_{ds_no_dashes}`
WHERE
  -- Greater than 30 minutes of inactivity defines a new session.
  (inactivity_minutes > 30 OR inactivity_minutes IS NULL)
  -- Only create new sessions using traces in the current execution date partition.
  -- This is necessary because the staging table overlaps the previous partition.
  AND date(block_timestamp) = date('{ds}');


-- Create the sessions table if necessary.
CREATE TABLE IF NOT EXISTS `{destination_project_id}.{destination_dataset_name}.sessions`
LIKE `stage_sessions_{ds_no_dashes}`;


-- Merge staging table with destination table.
MERGE INTO `{destination_project_id}.{destination_dataset_name}.sessions` AS target
USING `stage_sessions_{ds_no_dashes}` AS source
ON false
WHEN NOT MATCHED AND date(start_block_timestamp) = date('{ds}') THEN
INSERT (
  id,
  start_block_number,
  start_block_timestamp,
  wallet_address,
  contract_address
)
VALUES (
  id,
  start_block_number,
  start_block_timestamp,
  wallet_address,
  contract_address
)
WHEN NOT MATCHED BY SOURCE AND date(start_block_timestamp) = date('{ds}') THEN
DELETE;


-- Delete the temporary table.
DROP TABLE `stage_sessions_{ds_no_dashes}`;

-- Drop staging table for root call traces.
DROP TABLE `{destination_project_id}.{temp_dataset_name}.stage_root_call_traces_{ds_no_dashes}`;