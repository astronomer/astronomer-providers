ARG IMAGE_NAME
FROM ${IMAGE_NAME}

USER root
RUN apt-get update -y && apt-get install -y git
RUN apt-get install -y --no-install-recommends \
        build-essential \
        libsasl2-2 \
        libsasl2-dev \
        libsasl2-modules \
        jq

COPY setup.cfg ${AIRFLOW_HOME}/astronomer_providers/setup.cfg
COPY pyproject.toml ${AIRFLOW_HOME}/astronomer_providers/pyproject.toml

RUN pip install -e "${AIRFLOW_HOME}/astronomer_providers[all,tests,mypy]"
RUN pip install apache-airflow-providers-slack

COPY openlineage_python-0.30.2-py3-none-any.whl /tmp/openlineage_python-0.30.2-py3-none-any.whl
COPY openlineage_airflow-0.30.2-py3-none-any.whl /tmp/openlineage_airflow-0.30.2-py3-none-any.whl 

RUN pip install --no-dependencies /tmp/openlineage_python-0.30.2-py3-none-any.whl /tmp/openlineage_airflow-0.30.2-py3-none-any.whl

USER astro
