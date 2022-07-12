ARG IMAGE_NAME
FROM ${IMAGE_NAME}

USER root
RUN apt-get update -y && apt-get install -y git
RUN apt-get install -y --no-install-recommends \
        build-essential \
        libsasl2-2 \
        libsasl2-dev \
        libsasl2-modules

COPY setup.cfg ${AIRFLOW_HOME}/astronomer_providers/setup.cfg
COPY pyproject.toml ${AIRFLOW_HOME}/astronomer_providers/pyproject.toml


RUN pip install -e "${AIRFLOW_HOME}/astronomer_providers[all,tests,mypy]"
USER astro
