set -e
set -o xtrace
set -o pipefail

airflow_bucket=${1}

if [ -z "${airflow_bucket}" ]; then
    echo "Usage: $0 <airflow_bucket>"
    exit 1
fi

gsutil -m cp -r dags/* gs://${airflow_bucket}/dags/