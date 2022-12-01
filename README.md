# Ethereum ETL Airflow

Read this article: https://cloud.google.com/blog/products/data-analytics/ethereum-bigquery-how-we-built-dataset

## Setting up Airflow DAGs using Google Cloud Composer

### Create BigQuery Datasets

- Sign in to BigQuery https://bigquery.cloud.google.com/
- Create new datasets called `crypto_ethereum`, `crypto_ethereum_raw`, `crypto_ethereum_temp`

### Create Google Cloud Storage bucket

- Create a new Google Storage bucket to store exported files https://console.cloud.google.com/storage/browser

### Create Google Cloud Composer (version 2) environment

Create a new Cloud Composer environment:

```bash
export ENVIRONMENT_NAME=ethereum-etl-0

AIRFLOW_CONFIGS_ARR=(
    "celery-worker_concurrency=12"
    "core-parallelism=48"
    "scheduler-dag_dir_list_interval=300"
    "scheduler-min_file_process_interval=120"
)
export AIRFLOW_CONFIGS=$(IFS=, ; echo "${AIRFLOW_CONFIGS_ARR[*]}")

gcloud composer environments create \
    $ENVIRONMENT_NAME \
    --location=us-central1 \
    --image-version=composer-2.0.25-airflow-2.2.5 \
    --environment-size=medium \
    --scheduler-cpu=2 \
    --scheduler-memory=13 \
    --scheduler-storage=2 \
    --scheduler-count=2 \
    --web-server-cpu=1 \
    --web-server-memory=2 \
    --web-server-storage=1 \
    --worker-cpu=4 \
    --worker-memory=26 \
    --worker-storage=4 \
    --min-workers=1 \
    --max-workers=8 \
    --airflow-configs=$AIRFLOW_CONFIGS

gcloud composer environments update \
    $ENVIRONMENT_NAME \
    --location=us-central1 \
    --update-pypi-packages-from-file=requirements_airflow.txt
```

Create variables in Airflow (**Admin > Variables** in the UI):

| Variable                                | Description                             |
|-----------------------------------------|-----------------------------------------|
| ethereum_output_bucket                  | GCS bucket to store exported files      |
| ethereum_provider_uris                  | Comma separated URIs of Ethereum nodes  |
| ethereum_destination_dataset_project_id | Project ID of BigQuery datasets         |
| notification_emails                     | email for notifications                 |

Check other variables in `dags/ethereumetl_airflow/variables.py`.

### Upload DAGs

In order to install the DAGs you will need to install dependencies first otherwise the installation will fail. 
For this setup we recommend trying the following dependencies. You may have to adjust some to get your DAGs working. 

```eth-hash=0.3.3
web3==5.29.2
eth-abi==2.1.1
eth-rlp==0.2.1
ethereum-etl
```

```bash
> ./upload_dags.sh <airflow_bucket>
```

### Running Tests

```bash
pip install \
    -r requirements_test.txt \
    -r requirements_local.txt \
    -r requirements_airflow.txt
pytest -vv -s
```

### Running locally
A docker compose definition has been provided to easily spin up a local Airflow instance.

To build the required image:
```bash
docker compose build
```
To start Airflow:
```bash
docker compose up airflow
```

The instance requires the `CLOUDSDK_CORE_PROJECT` environment variable to be set in most cases. Airflow Variables can be defined in [variables.json](./docker/variables.json).

### Creating Table Definition Files for Parsing Events and Function Calls

Read this article: https://medium.com/@medvedev1088/query-ens-and-0x-events-with-sql-in-google-bigquery-4d197206e644

### More Information

You can follow the instructions here for Polygon DAGs https://github.com/blockchain-etl/polygon-etl. The architecture
there is very similar to Ethereum so in most case substituting `polygon` for `ethereum` will work. Contributions 
to this README file for porting documentation from Polygon to Ethereum are welcome.
