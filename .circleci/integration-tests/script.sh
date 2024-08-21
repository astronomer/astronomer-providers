#!/usr/bin/env bash

# Make the script exit with the status if one of the commands fails. Without this, the Airflow task calling this script
# will be marked as 'success' and the DAG will proceed on to the subsequent tasks.
set -e

# This script deploys to an already existing Astro Cloud Airflow deployment.
# It currently does not support creating a new deployment.
#
# Execute the script with below positional params
#         bash script.sh astro-cloud <DOCKER_REGISTRY> <ORGANIZATION_ID>  <DEPLOYMENT_ID> <ASTRONOMER_KEY_ID> <ASTRONOMER_KEY_SECRET>
#         - DOCKER_REGISTRY: Docker registry domain. Script will push the docker image here.
#         - ORGANIZATION_ID: Astro cloud deployment organization Id. Get it from UI.
#         - DEPLOYMENT_ID: Astro cloud deployment Id. Get it from UI.
#         - TOKEN: Astro workspace token.

SCRIPT_PATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
PROJECT_PATH=${SCRIPT_PATH}/../../
PROVIDER_PATH=${PROJECT_PATH}/astronomer/providers/

function echo_help() {
    echo "Usage:"
    echo "DEPLOYMENT_INSTANCE:    Deployment instance. Valid Value astro-cloud"
    echo "DOCKER_REGISTRY:        Docker registry"
    echo "ORGANIZATION_ID         Astro cloud organization Id"
    echo "DEPLOYMENT_ID           Astro cloud Deployment id"
    echo "TOKEN     Astro workspace token"
    echo "bash script.sh astro-cloud <DOCKER_REGISTRY> <ORGANIZATION_ID>  <DEPLOYMENT_ID> <TOKEN>"
}

# Delete if source old source files exist
function clean() {
  [[ -d ${SCRIPT_PATH}/astronomer-providers ]] && rm -rf "${SCRIPT_PATH}"/astronomer-providers
  [[ -f ${SCRIPT_PATH}/pyproject.toml ]] && rm "${SCRIPT_PATH}"/pyproject.toml
  [[ -f ${SCRIPT_PATH}/setup.cfg ]] && rm "${SCRIPT_PATH}"/setup.cfg
  [[ -f ${SCRIPT_PATH}/packages.txt ]] && rm "${SCRIPT_PATH}"/packages.txt
  [[ -f ${SCRIPT_PATH}/requirements.txt ]] && rm "${SCRIPT_PATH}"/requirements.txt
  find . -name "example_*" ! -name "example_databricks_workflow.py" -exec rm {} \;
}

if [ "$1" == "-h" ]; then
  echo_help
  exit
fi

DEPLOYMENT_INSTANCE=$1
DOCKER_REGISTRY=""
ORGANIZATION_ID=""
DEPLOYMENT_ID=""
TOKEN=""

if [[ ${DEPLOYMENT_INSTANCE} == "astro-cloud" ]]; then
  DOCKER_REGISTRY=$2
  ORGANIZATION_ID=$3
  DEPLOYMENT_ID=$4
  TOKEN=$5
else
  echo "Valid value for DEPLOYMENT_INSTANCE can only be astro-cloud"
  echo_help
  exit 1
fi

clean

# Copy source files
mkdir "${SCRIPT_PATH}"/astronomer-providers
cp -r "${PROJECT_PATH}"/astronomer "${SCRIPT_PATH}"/astronomer-providers
cp -r  "${PROJECT_PATH}"/pyproject.toml "${SCRIPT_PATH}"/astronomer-providers
cp -r  "${PROJECT_PATH}"/setup.cfg "${SCRIPT_PATH}"/astronomer-providers

echo "Listing contents of the databricks directory:"
ls -la databricks
# Copy examples
for dag in $(find "${PROVIDER_PATH}" -type f -name 'example_*'); do cp "${dag}" "${SCRIPT_PATH}"; done;

ls -la databricks
# Build image and deploy
BUILD_NUMBER=$(awk 'BEGIN {srand(); print srand()}')
if [[ ${DEPLOYMENT_INSTANCE} == "astro-cloud" ]]; then
  IMAGE_NAME=${DOCKER_REGISTRY}/${ORGANIZATION_ID}/${DEPLOYMENT_ID}:ci-${BUILD_NUMBER}
  docker build --platform=linux/amd64 -t "${IMAGE_NAME}" -f "${SCRIPT_PATH}"/Dockerfile.astro_cloud "${SCRIPT_PATH}"
  docker login "${DOCKER_REGISTRY}" -u "cli" -p "${TOKEN}"
  docker push "${IMAGE_NAME}"


  # Step 5. Create the Image
  echo "get image id"
  IMAGE=$( curl --location --request POST "https://api.astronomer-stage.io/hub/graphql" \
        --header "Authorization: Bearer "${TOKEN}"" \
        --header "Content-Type: application/json" \
        --data-raw "{
            \"query\" : \"mutation CreateImage(\n    \$input: CreateImageInput!\n) {\n    createImage (\n    input: \$input\n) {\n    id\n    tag\n    repository\n    digest\n    env\n    labels\n    deploymentId\n  }\n}\",
            \"variables\" : {
                \"input\" : {
                    \"deploymentId\" : \"$DEPLOYMENT_ID\",
                    \"tag\" : \"ci-$BUILD_NUMBER\"
                    }
                }
            }" | jq -r '.data.createImage.id')
  # Step 6. Deploy the Image
  echo "deploy image"

  curl --location --request POST "https://api.astronomer-stage.io/hub/graphql" \
          --header "Authorization: Bearer $TOKEN" \
          --header "Content-Type: application/json" \
          --data-raw "{
              \"query\" : \"mutation DeployImage(\n    \$input: DeployImageInput!\n  ) {\n    deployImage(\n      input: \$input\n    ) {\n      id\n      deploymentId\n      digest\n      env\n      labels\n      name\n      tag\n      repository\n    }\n}\",
              \"variables\" : {
                  \"input\" : {
                      \"deploymentId\" : \"$DEPLOYMENT_ID\",
                      \"imageId\" : \"$IMAGE\",
                      \"tag\" : \"ci-$BUILD_NUMBER\",
                      \"repository\" : \"images.astronomer-stage.cloud/$ORGANIZATION_ID/$DEPLOYMENT_ID\",
                      \"dagDeployEnabled\":false
                      }
                  }
            }"

fi

clean
