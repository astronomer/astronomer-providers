#!/usr/bin/env bash

SCRIPT_PATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
PROJECT_PATH=${SCRIPT_PATH}/../../
PROVIDER_PATH=${PROJECT_PATH}/astronomer/providers/

function echo_help() {
    echo "Usage:"
    echo "BASEDOMAIN:        Astro cloud instance domain"
    echo "RELEASE_NAME:      Astro release name of a deployment"
    echo "BUILD_NUMBER       Docker image tag"
    echo "API_KEY            Deployment service account api key"
    echo "sh script.sh <BASEDOMAIN> <RELEASE_NAME> <BUILD_NUMBER> <API_KEY>"
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

if [ "$1" == "-h" ] || [ "$#" -ne 4 ]; then
  echo_help
  exit
fi

BASE_DOMAIN=$1
RELEASE_NAME=$2
BUILD_NUMBER=$3
API_KEY=$4

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
IMAGE_NAME=registry.${BASE_DOMAIN}/${RELEASE_NAME}/airflow:ci-${BUILD_NUMBER}
docker build -t "${IMAGE_NAME}" -f "${SCRIPT_PATH}"/Dockerfile "${SCRIPT_PATH}"
docker login registry.staging.astronomer.io  -u _ -p "${API_KEY}"
docker push "${IMAGE_NAME}"

clean
