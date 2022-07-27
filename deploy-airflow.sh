#!/bin/bash
while [[ $# -gt 0 ]]; do
  case $1 in
    -n|--namespace)
      NAMESPACE="$2"
      shift # past argument
      shift # past value
      ;;
    -p|--pg-url)
      PG_URL="$2"
      shift # past argument
      shift # past value
      ;;
    -f|--fernet-key)
      FERNET_KEY="$2"
      shift # past argument
      shift # past value
      ;;
    -e|--ecs-host)
      ECS_HOST="$2"
      shift # past argument
      shift # past value
      ;;
    --image-name)
      IMAGE_NAME="$2"
      shift # past argument
      shift # past value
      ;;
    --image-tag)
      IMAGE_TAG="$2"
      shift # past argument
      shift # past value
      ;;
    --build-image)
      BUILD_IMAGE=true
      shift # past argument
      ;;
    -*|--*)
      echo "Unknown option $1"
      exit 1
      ;;
  esac
done

# Check all required arguments
if [[ -z "$NAMESPACE" || -z "$PG_URL" || -z "$ECS_HOST" || -z "$IMAGE_NAME" || -z "$IMAGE_TAG" || -z "$FERNET_KEY" ]];
then
  echo "You missed some required argument."
  exit 1
fi

# Prepare some arguments
PROJECT_DIR=$(cd $(dirname $0);pwd)
TEMP_DIR="$PROJECT_DIR"/.tmp
HELM_VALUE_YAML="$TEMP_DIR"/value.yaml
IMAGE_REPOSITORY="$ECS_HOST/$IMAGE_NAME"

if [ ! -z $BUILD_IMAGE ]
then
  # Create Repo in ECR
  aws ecr create-repository --repository-name "$IMAGE_REPOSITORY"

  # Login to ECR
  aws ecr get-login-password --region ap-south-1 | docker login --username AWS --password-stdin "$ECS_HOST"

  # Build and push the image
  docker buildx build \
    --platform linux/amd64 \
    --file Dockerfile \
    --no-cache \
    --load \
    -t "$IMAGE_REPOSITORY:$IMAGE_TAG" .

  # Check if build was success
  if [ $? -ne 0 ]
  then
    echo "Docker Build failed. Not proceeding."
    exit 1
  fi

  docker push "$IMAGE_REPOSITORY:$IMAGE_TAG"

  # Check if push was success
  if [ $? -ne 0 ]
  then
    echo "Docker Push failed. Not proceeding."
    exit 1
  fi

fi

# Create temp folder and write helm values yaml to it.
mkdir -p -- "$TEMP_DIR"

# shellcheck disable=SC2002
cat "$PROJECT_DIR"/helm-values.yaml | \
  sed "s={{IMAGE_REPOSITORY}}=$IMAGE_REPOSITORY=" | \
  sed "s={{IMAGE_TAG}}=$IMAGE_TAG=" | \
  sed "s/{{FERNET_KEY}}/$FERNET_KEY/" > "$HELM_VALUE_YAML"

# Recreate namespace and install all resources.
kubectl delete namespace "$NAMESPACE"
kubectl create namespace "$NAMESPACE"

kubectl create secret generic airflow-database --from-literal=connection=postgresql+psycopg2://"$PG_URL" -n "$NAMESPACE"

kubectl create secret generic airflow-result-database --from-literal=connection=db+postgresql://"$PG_URL" -n "$NAMESPACE"

kubectl create secret generic airflow-webserver-secret --from-literal="webserver-secret-key=$(python3 -c 'import secrets; print(secrets.token_hex(16))')" -n "$NAMESPACE"

helm upgrade --install airflow apache-airflow/airflow --namespace "$NAMESPACE" --create-namespace -f "$HELM_VALUE_YAML" --debug

# Clean up temp folder
rm -rf "$TEMP_DIR"
