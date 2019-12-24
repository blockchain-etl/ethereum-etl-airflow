CREATE TEMP FUNCTION clean_json_escape(dirty STRING) AS (replace(replace(dirty, "\\\\", "\\"), "\\'", "'"));
CREATE TEMP FUNCTION clean_byte_prefix(response STRING) AS (if(starts_with(response, 'b\'') and length(response) >= 3, substr(response, 3, length(response) - 3), response));
CREATE TEMP FUNCTION clean_response(response STRING) AS (clean_json_escape(clean_byte_prefix(response)));

select
    address,
    json_extract_scalar(clean_response(response), "$.result[0].ContractName") as contract_name,
    json_extract_scalar(clean_response(response), "$.result[0].ABI") as abi,
    json_extract_scalar(clean_response(response), "$.result[0].SourceCode") as source_code,
FROM {{params.dataset_name_raw}}.contract_codes AS contract_codes

