ARG IMAGE_NAME="quay.io/astronomer/ap-airflow:2.2.3"
FROM ${IMAGE_NAME}

USER root
COPY astronomer/ ${AIRFLOW_HOME}/astronomer_providers/astronomer/
COPY setup.cfg ${AIRFLOW_HOME}/astronomer_providers/setup.cfg
COPY pyproject.toml ${AIRFLOW_HOME}/astronomer_providers/pyproject.toml

RUN pip install -e ${AIRFLOW_HOME}/astronomer_providers
USER astro
