ARG IMAGE_NAME="quay.io/astronomer/astro-runtime:5.0.0-base"
FROM ${IMAGE_NAME}

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libsasl2-2 \
        libsasl2-dev \
        libsasl2-modules \
        libkrb5-dev \
        krb5-user \
        kinit \
        git \
        ssh \
        vim \
        gcc \
        g++

##################### Hive ###################

ADD ./jdk-8u202-linux-x64.tar.gz /usr/local
ENV JAVA_HOME=/usr/local/jdk1.8.0_202

ADD ./hadoop-2.10.1.tar.gz /usr/local
ADD ./apache-hive-2.3.9-bin.tar.gz /usr/local
ADD ./krb5.conf /etc/krb5.conf
# ADD ./providers_team_keypair.pem /usr/local

COPY ./hive-site.xml /usr/local/apache-hive-2.3.9-bin/conf
ENV HADOOP_HOME=/usr/local/hadoop-2.10.1
ENV HIVE_HOME=/usr/local/apache-hive-2.3.9-bin
ENV HADOOP_CONF_DIR=/usr/local/hadoop/conf

ENV PATH $PATH:$JAVA_HOME/bin:$HIVE_HOME:$HIVE_HOME/bin:$HADOOP_HOME:$HADOOP_HOME/bin:$SPARK_HOME:$SPARK_HOME/bin:$HADOOP_CONF_DIR


RUN echo $(ls -lrt /usr/local)
ENV AIRFLOW__CORE__SECURITY kerberos
ENV AIRFLOW__KERBEROS__KEYTAB /usr/local/airflow/dags/airflow.keytab


##################### Hive ###################



COPY setup.cfg ${AIRFLOW_HOME}/astronomer_providers/setup.cfg
COPY pyproject.toml ${AIRFLOW_HOME}/astronomer_providers/pyproject.toml

RUN pip install -e "${AIRFLOW_HOME}/astronomer_providers[all,tests,mypy]"
USER astro
