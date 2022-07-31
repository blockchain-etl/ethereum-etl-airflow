# Ethereum ETL Airflow

Read this article: [https://cloud.google.com/blog/products/data-analytics/ethereum-bigquery-how-we-built-dataset](https://cloud.google.com/blog/products/data-analytics/ethereum-bigquery-how-we-built-dataset)

## Support

The repo itself can be deployed on any compute cloud platform. 
Certain DAGs are available only for GCP.


| Feature | GCP | AWS | 
|---------|-------|-------|
| Can be deployed? | Yes | Yes |
| ethereum_amend_dag | Yes | No |
| ethereum_clean_dag | Yes ? | Yes ? |
| ethereum_export_dag | Yes (Provider → BigQuery)| Yes (Provider → S3) |
| ethereum_load_dag | Yes (BigQuery → Storage) | Yes (S3 → RedShift) |
| ethereum_parse_dag | Yes | No |
| ethereum_partition_dag | Yes | No |
| ethereum_sessions_dag | Yes | No |
------------------------------------


# Setting up Airflow DAGs

## Google Cloud Composer

Assumes that you have `gcloud` installed and configured. If not, [install Google Cloud CLI](https://cloud.google.com/sdk/docs/install-sdk)


Create a new Cloud Composer environment:

```bash
export ENVIRONMENT_NAME=ethereum-etl-0
export NODE_TYPE=n1-standard-2
export ZONE=us-central1-a
gcloud composer environments create $ENVIRONMENT_NAME \
	--location=us-central1 \
	--zone=$ZONE \
   	--disk-size=50GB \
	--machine-type=$NODE_TYPE \
	--node-count=3 \
	--python-version=3 \
	--image-version=composer-1.17.6-airflow-1.10.15 \
	--network=default \
	--subnetwork=default

gcloud composer environments update $ENVIRONMENT_NAME \
	--location=us-central1 \
	--update-pypi-package=ethereum-etl==1.7.2
```

### Upload DAGs

```bash
> ./upload_dags.sh <airflow_bucket>
```

## AWS EKS

Assumes you have docker, kubectl, helm, eksctl, aws cli installed and configured.
The ECR Repository is created if it does not exists.

Airflow comes with its own Postgres container as well. For most purposes, 
an external PG connection is recommended. 
You should set it up, as it will be required.


```bash
export ENVIRONMENT_NAME=ethereum-etl-0
export NODE_TYPE=m5.large
export ZONE=ap-south-1
eksctl create cluster \
	--name $ENVIRONMENT_NAME  \
	--region $ZONE
eksctl create nodegroup \
	--cluster $ENVIRONMENT_NAME  \
	--region $ZONE \
	--name $ENVIRONMENT_NAME-nodegroup \
	--node-type $NODE_TYPE \
	--nodes 3 \
	--nodes-min 2 \
	--nodes-max 10
./deploy-airflow.sh \
	-n $ENVIRONMENT_NAME \
	--pg-url USER:PASSWORD@HOST:PORT/DB \
	--ecs-host 289344454031.dkr.ecr.ap-south-1.amazonaws.com  \
	--image-name ethetl \
	--image-tag latest  \
	--build-image \
	--fernet-key 15NrZQ5lfysmX9HggBJgl8qlFVrsTys8-XJcK_qN4hQ=
```

You might also want to change the policy of storage for airflow-worker-logs to be retained when you redeploy. In order to to this, follow the [retain volume steps](https://kubernetes.io/docs/tasks/administer-cluster/change-pv-reclaim-policy/).

To enable port forwarding from this app so you can access Airflow UI, run

`kubectl port-forward svc/airflow-webserver 8080:8080 --namespace $ENVIRONMENT_NAME`. 

You can now login to [http://localhost:8080](http://localhost:8080/).


# Creating variables

Create variables by following steps on [variables](docs/Variables.md) and importing them to Airflow UI. 

# Creating Connections

You will need to [create connections in Airflow UI](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html#creating-a-connection-with-the-ui) for connecting to the cloud
provider of your choice.

## GCP

1. If you want to use GCP for storage and processing of the data, then
create a service account for your GCP app. 
[Follow the instructions](https://cloud.google.com/docs/authentication/production). 
Store the json file somewhere secure. You will need it's content for next step.
2. You'll need to create the following connection IDs. 
[Refer here for details on specifics](https://airflow.apache.org/docs/apache-airflow/1.10.13/howto/connection/gcp.html). Copy the content of above JSON file in the field `Keyfile JSON`.

- `google_cloud_default`
- `bigquery_default`

# Starting DAGs

Once in the Airflow UI, make sure to start the following DAGs

- airflow_monitoringq	
- ethereum_amend_dag
- ethereum_clean_dag
- ethereum_export_dag
- ethereum_load_dag
- ethereum_partition_dag
- ethereum_sessions_dag
- ethereum_verify_streaming_dag

There are 120+ other DAGs that parse contract specific logic. You can optionally chose to start some or all or none of them.

# Running Tests

```bash
pip install -r requirements.txt
export PYTHONPATH='dags'
pytest -vv -s
```

# Creating Table Definition Files for Parsing Events and Function Calls

Read this article: [https://medium.com/@medvedev1088/query-ens-and-0x-events-with-sql-in-google-bigquery-4d197206e644](https://medium.com/@medvedev1088/query-ens-and-0x-events-with-sql-in-google-bigquery-4d197206e644)

# More Information

You can follow the instructions here for Polygon DAGs [https://github.com/blockchain-etl/polygon-etl](https://github.com/blockchain-etl/polygon-etl). The architecture
there is very similar to Ethereum so in most case substituting `polygon` for `ethereum` will work. Contributions 
to this README file for porting documentation from Polygon to Ethereum are welcome.