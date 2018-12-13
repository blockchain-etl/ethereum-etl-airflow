# Ethereum ETL Airflow

Read this article: https://cloud.google.com/blog/products/data-analytics/ethereum-bigquery-how-we-built-dataset

To upload CSVs to BigQuery:

- Install Google Cloud SDK https://cloud.google.com/sdk/docs/quickstart-debian-ubuntu

- Create a new Google Storage bucket https://console.cloud.google.com/storage/browser

- Upload the files:

```bash
> cd output
> gsutil -m rsync -r . gs://<your_bucket>/ethereumetl/export
```

- Sign in to BigQuery https://bigquery.cloud.google.com/

- Create a new dataset called `ethereum_blockchain_raw` and `ethereum_blockchain`

- Load the files from the bucket to BigQuery:

```bash
> git clone https://github.com/medvedev1088/ethereum-etl-airflow.git
> cd ethereum-etl-airflow/dags/resources/stages
> bq --location=US load --replace --source_format=CSV --skip_leading_rows=1 ethereum_blockchain_raw.blocks gs://<your_bucket>/ethereumetl/export/blocks/*.csv ./raw/schemas/blocks.json
> bq --location=US load --replace --source_format=CSV --skip_leading_rows=1 ethereum_blockchain_raw.transactions gs://<your_bucket>/ethereumetl/export/transactions/*.csv ./raw/schemas/transactions.json
> bq --location=US load --replace --source_format=CSV --skip_leading_rows=1 ethereum_blockchain_raw.token_transfers gs://<your_bucket>/ethereumetl/export/token_transfers/*.csv ./raw/schemas/token_transfers.json
> bq --location=US load --replace --source_format=CSV --skip_leading_rows=1 ethereum_blockchain_raw.receipts gs://<your_bucket>/ethereumetl/export/receipts/*.csv ./raw/schemas/receipts.json
> bq --location=US load --replace --source_format=NEWLINE_DELIMITED_JSON ethereum_blockchain_raw.logs gs://<your_bucket>/ethereumetl/export/logs/*.json ./raw/schemas/logs.json
> bq --location=US load --replace --source_format=NEWLINE_DELIMITED_JSON ethereum_blockchain_raw.contracts gs://<your_bucket>/ethereumetl/export/contracts/*.json ./raw/schemas/contracts.json
> bq --location=US load --replace --source_format=CSV --skip_leading_rows=1 --allow_quoted_newlines ethereum_blockchain_raw.tokens gs://<your_bucket>/ethereumetl/export/tokens/*.csv ./raw/schemas/tokens.json
```

Note that NEWLINE_DELIMITED_JSON is used to support REPEATED mode for the columns with lists.

Enrich `blocks`:

```bash
> bq mk --table --description "$(cat ./enrich/descriptions/blocks.txt | tr '\n' ' ')" --time_partitioning_field timestamp ethereum_blockchain.blocks ./enrich/schemas/blocks.json
> bq --location=US query --destination_table ethereum_blockchain.blocks --use_legacy_sql=false "$(cat ./enrich/sqls/blocks.sql | tr '\n' ' ')"
```

Enrich `transactions`:

```bash
> bq mk --table --description "$(cat ./enrich/descriptions/transactions.txt | tr '\n' ' ')" --time_partitioning_field block_timestamp ethereum_blockchain.transactions ./enrich/schemas/transactions.json
> bq --location=US query --destination_table ethereum_blockchain.transactions --use_legacy_sql=false "$(cat ./enrich/sqls/transactions.sql | tr '\n' ' ')"
```

Enrich `token_transfers`:

```bash
> bq mk --table --description "$(cat ./enrich/descriptions/token_transfers.txt | tr '\n' ' ')" --time_partitioning_field block_timestamp ethereum_blockchain.token_transfers ./enrich/schemas/token_transfers.json
> bq --location=US query --destination_table ethereum_blockchain.token_transfers --use_legacy_sql=false "$(cat ./enrich/sqls/token_transfers.sql | tr '\n' ' ')"
```

Enrich `logs`:

```bash
> bq mk --table --description "$(cat ./enrich/descriptions/logs.txt | tr '\n' ' ')" --time_partitioning_field block_timestamp ethereum_blockchain.logs ./enrich/schemas/logs.json
> bq --location=US query --destination_table ethereum_blockchain.logs --use_legacy_sql=false "$(cat ./enrich/sqls/logs.sql | tr '\n' ' ')"
```

Enrich `contracts`:

```bash
> bq mk --table --description "$(cat ./enrich/descriptions/contracts.txt | tr '\n' ' ')" --time_partitioning_field block_timestamp ethereum_blockchain.contracts ./enrich/schemas/contracts.json
> bq --location=US query --destination_table ethereum_blockchain.contracts --use_legacy_sql=false "$(cat ./enrich/sqls/contracts.sql | tr '\n' ' ')"
```

Enrich `tokens`:

```bash
> bq mk --table --description "$(cat ./enrich/descriptions/tokens.txt | tr '\n' ' ')" ethereum_blockchain.tokens ./enrich/schemas/tokens.json
> bq --location=US query --destination_table ethereum_blockchain.tokens --use_legacy_sql=false "$(cat ./enrich/sqls/tokens.sql | tr '\n' ' ')"
```

## Setting up Airflow DAGs using Google Cloud Composer

### Create BigQuery Datasets

- Sign in to BigQuery https://bigquery.cloud.google.com/
- Create a new datasets called `ethereum_blockchain`, `ethereum_blockchain_raw`, `ethereum_blockchain_temp`

### Create Google Cloud Storage bucket

- Create a new Google Storage bucket to store exported files https://console.cloud.google.com/storage/browser

### Create Google Cloud Composer environment

Create environment here, https://console.cloud.google.com/composer, use Python version 3, set up environment variables:

+--------------------------------+-----------------------------------------+
| Variable                       | Description                             |
+--------------------------------+-----------------------------------------+
| OUTPUT_BUCKET                  | GCS bucket to store exported files      |
| EXPORT_BLOCKS_AND_TRANSACTIONS | True or False                           |
| EXPORT_RECEIPTS_AND_LOGS       | True or False                           |
| EXTRACT_TOKEN_TRANSFERS        | True or False                           |
| EXPORT_CONTRACTS               | True or False                           |
| EXPORT_TOKENS                  | True or False                           |
| EXPORT_TRACES                  | True or False                           |
| WEB3_PROVIDER_URI_ARCHIVAL     | URI of archival Parity node for traces  |
| NOTIFICATION_EMAILS            | email for notifications                 |
| EXPORT_MAX_WORKERS             | max workers for export (10 recommended) |
| EXPORT_BATCH_SIZE              | batch size for export (10 recommended)  |
| WEB3_PROVIDER_URI_BACKUP       | URI for backup node                     |
| DESTINATION_DATASET_PROJECT_ID | Project ID of BigQuery datasets         |
+--------------------------------+-----------------------------------------+

### Upload dags

```bash
> ./upload_dags.sh <airflow_bucket>
```