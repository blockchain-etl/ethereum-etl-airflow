# Airflow Variables

There are many variables that you need to set before the first successful run.
A sample [variables.example.json](../variables.example.json) is provided. You should copy
the file and rename it to `variables.json` and use it to import in the
[airflow variables](http://localhost:8080/variable/list/) UI.

This document describes various keys of the json file, and the steps required to
setup them.

# Cloud Provider

Regardless of where you have deployed your app (aws or gcp), you can choose 
which platform will be used for storing and processing of ETL'd data.


## ethereum_cloud_provider

Ethereum-ETL Supports two cloud providres - aws and gcp. The default is `gcp`.
You can set this by

```json
	"ethereum_cloud_provider: "aws",
```

or

```json
	"ethereum_cloud_provider": "gcp",
```

## ethereum_destination_dataset_project_id

Project ID of BigQuery or Redshift datasets.

### BigQuery

If you don't have a project created, create one. 
Within this project, create new datasets called 
`crypto_ethereum`, `crypto_ethereum_raw` and `crypto_ethereum_temp`.
Copy the ID of this project to be used in this variable.

### AWS

eth-redshift.cjhr02tuz8yb.ap-south-1.redshift.amazonaws.com:5439/dev


## ethereum_output_bucket

GCS or S3 Bucket to store exported files.

Create a new Google Storage bucket to store exported files [https://console.cloud.google.com/storage/browser](https://console.cloud.google.com/storage/browser)

Create a new AWS S3 bucket to store exported files.

There are additional cloud specific steps to
ensure that airflow app is able to write and read in this storage bucket.
Those are not mentioned here.


```json
	"ethereum_output_bucket": "dev-etl-01",
```

# Web3 Providers

## ethereum_provider_uris

A Comma separated URIs of Ethereum nodes.

```json
	"ethereum_provider_uris": "https://eth-mainnet.g.alchemy.com/v2/ALCHEMY_API",
```

## ethereum_price_provider_key

The key of Price provider. 

```json
	"ethereum_price_provider_key": "YOUR_KEY",
```

# Ethereum Configs

These are optional and by default set to `false`. Set them to `true` as per your needs. The key names are self explanatory.

```json
	"ethereum_export_daofork_traces_option": false,
	"ethereum_export_genesis_traces_option": false,
	"ethereum_export_blocks_and_transactions_toggle": false,
	"ethereum_export_receipts_and_logs_toggle":  false,
	"ethereum_extract_contracts_toggle": false,
	"ethereum_extract_tokens_toggle": false,
	"ethereum_extract_token_transfers_toggle": false,
	"ethereum_export_traces_toggle": false,
	"ethereum_load_start_date": "2022-01-01"
```

# Notification

## notification_emails

Comma seperated email for notifications

```json
	"notification_emails": "test@example.com,test2@example.com",
```

# Execution Options


```json
	"ethereum_max_lag_in_minutes": 1,
	"ethereum_export_batch_size": 150,
	"ethereum_export_max_active_runs": 2,
	"ethereum_export_max_workers": 2,
	"ethereum_export_prices_usd_toggle": "False",
	"ethereum_export_retries": 1,
	"ethereum_export_schedule_interval": "0 0 * * *",

```

# Optimisation

## ethereum_load_all_partitions

```json
	"ethereum_load_all_partitions": false,
```