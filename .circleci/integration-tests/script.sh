#!/usr/bin/env bash
# This script use to update a staging Astro staging and cloud Airflow deployment.
# It currently does not support creating a new deployment.
# To deploy on 'staging' execute the script with below positional params
#         bash script.sh staging <DOCKER_REGISTRY> <RELEASE_NAME>  <ASTRO_API_KEY>
#         - DOCKER_REGISTRY: Docker registry domain. Script will push the docker image here.
#         - RELEASE_NAME: Staging deployment release name. Get it from staging deployment UI.
#         - ASTRO_API_KEY: Staging deployment service account API key.
#
# To deploy on 'Astro Cloud' execute the script with below positional params
#         bash script.sh astro-cloud <DOCKER_REGISTRY> <ORGANIZATION_ID>  <DEPLOYMENT_ID> <ASTRONOMER_KEY_ID> <ASTRONOMER_KEY_SECRET>
#         - DOCKER_REGISTRY: Docker registry domain. Script will push the docker image here.
#         - ORGANIZATION_ID: Astro cloud deployment organization Id. Get it from UI.
#         - DEPLOYMENT_ID: Astro cloud deployment Id. Get it from UI.
#         - ASTRONOMER_KEY_ID: Astro cloud deployment service account API key Id.
#         - ASTRONOMER_KEY_SECRET: Astro cloud deployment service account API key secret.

SCRIPT_PATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
PROJECT_PATH=${SCRIPT_PATH}/../../
PROVIDER_PATH=${PROJECT_PATH}/astronomer/providers/

function echo_help() {
    echo "Usage:"
    echo "DEPLOYMENT_INSTANCE:    Deployment instance. Valid Value staging | astro-cloud"
    echo "DOCKER_REGISTRY:        Docker registry"
    echo "RELEASE_NAME:           Release name of the deployment"
    echo "ASTRONOMER_API_KEY      Deployment service account API key"
    echo "ORGANIZATION_ID         Astro cloud organization Id"
    echo "DEPLOYMENT_ID           Astro cloud Deployment id"
    echo "ASTRONOMER_KEY_ID       Astro cloud service account API key id"
    echo "ASTRONOMER_KEY_SECRET   Astro cloud service account API key secret"
    echo "bash script.sh staging <DOCKER_REGISTRY> <RELEASE_NAME>  <ASTRO_API_KEY>"
    echo "bash script.sh astro-cloud <DOCKER_REGISTRY> <ORGANIZATION_ID>  <DEPLOYMENT_ID> <ASTRONOMER_KEY_ID> <ASTRONOMER_KEY_SECRET>"
}

# Delete if source old source files exist
function clean() {
  [[ -d ${SCRIPT_PATH}/astronomer-providers ]] && rm -rf "${SCRIPT_PATH}"/astronomer-providers
  [[ -f ${SCRIPT_PATH}/pyproject.toml ]] && rm "${SCRIPT_PATH}"/pyproject.toml
  [[ -f ${SCRIPT_PATH}/setup.cfg ]] && rm "${SCRIPT_PATH}"/setup.cfg
  [[ -f ${SCRIPT_PATH}/packages.txt ]] && rm "${SCRIPT_PATH}"/packages.txt
  [[ -f ${SCRIPT_PATH}/requirements.txt ]] && rm "${SCRIPT_PATH}"/requirements.txt
  find  . -name "example_*" -exec rm {} \;
}

if [ "$1" == "-h" ]; then
  echo_help
  exit
fi

DEPLOYMENT_INSTANCE=$1
# Variable for Staging deployment
DOCKER_REGISTRY=""
RELEASE_NAME=""
ASTRONOMER_API_KEY=""
# Variable for astro-cloud deployment
ORGANIZATION_ID=""
DEPLOYMENT_ID=""
ASTRONOMER_KEY_ID=""
ASTRONOMER_KEY_SECRET=""

if [[ ${DEPLOYMENT_INSTANCE} == "staging" ]]; then
  DOCKER_REGISTRY=$2
  RELEASE_NAME=$3
  ASTRONOMER_API_KEY=$4
elif [[ ${DEPLOYMENT_INSTANCE} == "astro-cloud" ]]; then
  DOCKER_REGISTRY=$2
  ORGANIZATION_ID=$3
  DEPLOYMENT_ID=$4
  ASTRONOMER_KEY_ID=$5
  ASTRONOMER_KEY_SECRET=$6
else
  echo "DEPLOYMENT_INSTANCE can be either staging or astro-cloud"
  echo_help
  exit 1
fi

clean

# Copy source files
mkdir "${SCRIPT_PATH}"/astronomer-providers
cp -r "${PROJECT_PATH}"/astronomer "${SCRIPT_PATH}"/astronomer-providers
cp -r  "${PROJECT_PATH}"/pyproject.toml "${SCRIPT_PATH}"/astronomer-providers
cp -r  "${PROJECT_PATH}"/setup.cfg "${SCRIPT_PATH}"/astronomer-providers
touch packages.txt
touch requirements.txt

# Copy examples
for dag in $(find "${PROVIDER_PATH}" -type f -name 'example_*'); do cp "${dag}" "${SCRIPT_PATH}"; done;

# Build image and deploy
BUILD_NUMBER=$(awk 'BEGIN {srand(); print srand()}')
if [[ ${DEPLOYMENT_INSTANCE} == "staging" ]]; then
  IMAGE_NAME=registry.${DOCKER_REGISTRY}/${RELEASE_NAME}/airflow:ci-${BUILD_NUMBER}
  docker build --target "${DEPLOYMENT_INSTANCE}" -t "${IMAGE_NAME}" -f "${SCRIPT_PATH}"/Dockerfile "${SCRIPT_PATH}"
  docker login registry."${DOCKER_REGISTRY}"  -u _ -p "${ASTRONOMER_API_KEY}"
  docker push "${IMAGE_NAME}"
elif [[ ${DEPLOYMENT_INSTANCE} == "astro-cloud" ]]; then
  IMAGE_NAME=${DOCKER_REGISTRY}/${ORGANIZATION_ID}/${DEPLOYMENT_ID}:ci-${BUILD_NUMBER}
  docker build --target "${DEPLOYMENT_INSTANCE}" -t "${IMAGE_NAME}" -f "${SCRIPT_PATH}"/Dockerfile "${SCRIPT_PATH}"
  docker login "${DOCKER_REGISTRY}" -u "${ASTRONOMER_KEY_ID}" -p "${ASTRONOMER_KEY_SECRET}"
  docker push "${IMAGE_NAME}"

  TOKEN=$( curl --location --request POST "https://auth.astronomer.io/oauth/token" \
        --header "content-type: application/json" \
        --data-raw "{
            \"client_id\": \"$ASTRONOMER_KEY_ID\",
            \"client_secret\": \"$ASTRONOMER_KEY_SECRET\",
            \"audience\": \"astronomer-ee\",
            \"grant_type\": \"client_credentials\"}" | jq -r '.access_token' )
  # Step 5. Create the Image
  echo "get image id"
  IMAGE=$( curl --location --request POST "https://api.astronomer.io/hub/v1" \
        --header "Authorization: Bearer $TOKEN" \
        --header "Content-Type: application/json" \
        --data-raw "{
            \"query\" : \"mutation imageCreate(\n    \$input: ImageCreateInput!\n) {\n    imageCreate (\n    input: \$input\n) {\n    id\n    tag\n    repository\n    digest\n    env\n    labels\n    deploymentId\n  }\n}\",
            \"variables\" : {
                \"input\" : {
                    \"deploymentId\" : \"$DEPLOYMENT_ID\",
                    \"tag\" : \"ci-$BUILD_NUMBER\"
                    }
                }
            }" | jq -r '.data.imageCreate.id')
  # Step 6. Deploy the Image
  echo "deploy image"
  curl --location --request POST "https://api.astronomer.io/hub/v1" \
          --header "Authorization: Bearer $TOKEN" \
          --header "Content-Type: application/json" \
          --data-raw "{
              \"query\" : \"mutation imageDeploy(\n    \$input: ImageDeployInput!\n  ) {\n    imageDeploy(\n      input: \$input\n    ) {\n      id\n      deploymentId\n      digest\n      env\n      labels\n      name\n      tag\n      repository\n    }\n}\",
              \"variables\" : {
                  \"input\" : {
                      \"id\" : \"$IMAGE\",
                      \"tag\" : \"ci-$BUILD_NUMBER\",
                      \"repository\" : \"images.astronomer.cloud/$ORGANIZATION_ID/$DEPLOYMENT_ID\"
                      }
                  }
            }"
fi

clean
