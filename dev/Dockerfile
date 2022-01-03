ARG IMAGE_NAME="quay.io/astronomer/ap-airflow:2.2.3"
FROM ${IMAGE_NAME}

USER root
COPY astronomer_operators ${AIRFLOW_HOME}/astronomer_operators/astronomer_operators
COPY setup.cfg ${AIRFLOW_HOME}/astronomer_operators/setup.cfg
COPY setup.py ${AIRFLOW_HOME}/astronomer_operators/setup.py

RUN pip install -e ${AIRFLOW_HOME}/astronomer_operators
USER astro
