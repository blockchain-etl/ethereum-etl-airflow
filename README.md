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
    "celery-worker_concurrency=8"
    "scheduler-dag_dir_list_interval=300"
    "scheduler-min_file_process_interval=120"
)
export AIRFLOW_CONFIGS=$(IFS=, ; echo "${AIRFLOW_CONFIGS_ARR[*]}")

gcloud composer environments create \
    $ENVIRONMENT_NAME \
    --location=us-central1 \
    --image-version=composer-2.1.14-airflow-2.5.1 \
    --environment-size=medium \
    --scheduler-cpu=2 \
    --scheduler-memory=13 \
    --scheduler-storage=1 \
    --scheduler-count=1 \
    --web-server-cpu=1 \
    --web-server-memory=2 \
    --web-server-storage=512MB \
    --worker-cpu=2 \
    --worker-memory=13 \
    --worker-storage=10 \
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

### Updating package requirements
Suggested package requirements for Composer are stored in `requirements_airflow.txt`.

You can update the Composer environment using the following script:
```bash
ENVIRONMENT_NAME="ethereum-etl-0"
LOCAL_REQUIREMENTS_PATH="$(mktemp)"

# grep pattern removes comments and whitespace:
cat "./requirements_airflow.txt" | grep -o '^[^#| ]*' > "$LOCAL_REQUIREMENTS_PATH"

gcloud composer environments update \
  "$ENVIRONMENT_NAME" \
  --location="us-central1" \
  --update-pypi-packages-from-file="$LOCAL_REQUIREMENTS_PATH"
```

**Note:** Composer can be _very_ pedantic about conflicts in additional packages. You may have to fix dependency conflicts where you had no issues testing locally (when updating dependencies, Composer does something "cleverer" than just `pip install -r requirements.txt`). This is why `eth-hash` is currently pinned in `requirements_airflow.txt`. Typically we have found that pinning `eth-hash` and/or `eth-rlp` may make things work, though Your Mileage May Vary.

See [this issue](https://github.com/blockchain-etl/ethereum-etl-airflow/issues/481#issuecomment-1332878533) for further ideas on how to unblock problems you may encounter.

### Upload DAGs

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
