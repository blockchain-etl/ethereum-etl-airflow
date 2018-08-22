# Ethereum ETL Airflow

Refer to https://github.com/medvedev1088/ethereum-etl for exporting the data.

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
> bq --location=US load --replace --source_format=CSV --skip_leading_rows=1 --allow_quoted_newlines ethereum_blockchain_raw.tokens_duplicates gs://<your_bucket>/ethereumetl/export/tokens/*.csv ./raw/schemas/tokens.json
```

Note that NEWLINE_DELIMITED_JSON is used to support REPEATED mode for the columns with lists.

Enrich `blocks`:

```bash
> bq mk --table --description "$(cat ./enrich/schemas/descriptions/blocks.txt | tr '\n' ' ')" --time_partitioning_field timestamp ethereum_blockchain.blocks ./enrich/schemas/blocks.json
> bq --location=US query --destination_table ethereum_blockchain.blocks --use_legacy_sql=false "$(cat ./enrich/schemas/sqls/blocks.sql | tr '\n' ' ')"
```

Enrich `transactions`:

```bash
> bq mk --table --description "$(cat ./enrich/schemas/descriptions/transactions.txt | tr '\n' ' ')" --time_partitioning_field block_timestamp ethereum_blockchain.transactions ./enrich/schemas/transactions.json
> bq --location=US query --destination_table ethereum_blockchain.transactions --use_legacy_sql=false "$(cat ./enrich/schemas/sqls/transactions.sql | tr '\n' ' ')"
```

Enrich `token_transfers`:

```bash
> bq mk --table --description "$(cat ./enrich/schemas/descriptions/token_transfers.txt | tr '\n' ' ')" --time_partitioning_field block_timestamp ethereum_blockchain.token_transfers ./enrich/schemas/token_transfers.json
> bq --location=US query --destination_table ethereum_blockchain.token_transfers --use_legacy_sql=false "$(cat ./enrich/schemas/sqls/token_transfers.sql | tr '\n' ' ')"
```

Enrich `logs`:

```bash
> bq mk --table --description "$(cat ./enrich/schemas/descriptions/logs.txt | tr '\n' ' ')" --time_partitioning_field block_timestamp ethereum_blockchain.logs ./enrich/schemas/logs.json
> bq --location=US query --destination_table ethereum_blockchain.logs --use_legacy_sql=false "$(cat ./enrich/schemas/sqls/logs.sql | tr '\n' ' ')"
```

Enrich `contracts`:

```bash
> bq mk --table --description "$(cat ./enrich/schemas/descriptions/contracts.txt | tr '\n' ' ')" --time_partitioning_field block_timestamp ethereum_blockchain.contracts ./enrich/schemas/contracts.json
> bq --location=US query --destination_table ethereum_blockchain.contracts --use_legacy_sql=false "$(cat ./enrich/schemas/sqls/contracts.sql | tr '\n' ' ')"
```

Enrich `tokens`:

```bash
> bq mk --table --description "$(cat ./enrich/schemas/descriptions/tokens.txt | tr '\n' ' ')" ethereum_blockchain.tokens ./enrich/schemas/tokens.json
> bq --location=US query --destination_table ethereum_blockchain.tokens --use_legacy_sql=false "$(cat ./enrich/schemas/sqls/tokens.sql | tr '\n' ' ')"
```