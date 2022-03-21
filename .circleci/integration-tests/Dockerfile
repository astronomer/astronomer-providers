FROM quay.io/astronomer/ap-airflow:2.2.4

ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False

USER root
RUN apt-get update -y \
    && apt-get install -y git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY astronomer-providers /tmp/astronomer-providers
RUN pip install /tmp/astronomer-providers[all]
RUN pip install apache-airflow[slack]


RUN mkdir -p ${AIRFLOW_HOME}/dags
COPY . .
RUN cp -r example_* ${AIRFLOW_HOME}/dags
RUN cp master_dag.py  ${AIRFLOW_HOME}/dags/

USER astro
