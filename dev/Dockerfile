ARG IMAGE_NAME
FROM ${IMAGE_NAME}

USER root
RUN apt-get update -y && apt-get install -y git
RUN apt-get install -y --no-install-recommends \
    build-essential \
    libsasl2-2 \
    libsasl2-dev \
    libsasl2-modules \
    jq \
    pkg-config libxml2-dev libxmlsec1-dev libxmlsec1-openssl

COPY setup.cfg ${AIRFLOW_HOME}/astronomer_providers/setup.cfg
COPY pyproject.toml ${AIRFLOW_HOME}/astronomer_providers/pyproject.toml

COPY databricks  ${AIRFLOW_HOME}/dags/
RUN pip install -e "${AIRFLOW_HOME}/astronomer_providers[all,tests,mypy]"
RUN pip install apache-airflow-providers-slack

# Enable test connection for increasing local development productivity
ENV AIRFLOW__CORE__TEST_CONNECTION=Enabled

USER astro
