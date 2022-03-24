ARG IMAGE_NAME="quay.io/astronomer/astro-runtime:4.1.0-base"
FROM ${IMAGE_NAME}

USER root
RUN apt-get update -y && apt-get install -y git

#RUN apt-get install -y software-properties-common
#RUN apt-get update && apt-get install -y  openjdk-11-jdk
#RUN add-apt-repository ppa:openjdk-r/ppa

#RUN apt-get update && apt-get install -y  openjdk-8-jdk

RUN apt-get update && apt-get install -y gnupg2
RUN sudo apt install -y software-properties-common
RUN apt-get update && apt-get install -y wget procps && wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | sudo apt-key add -

RUN sudo add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/

RUN sudo apt-get update && sudo apt-get install -y adoptopenjdk-8-hotspot


RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       libsasl2-2 \
       libsasl2-dev \
       libsasl2-modules;

#ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64/
ENV JAVA_HOME=/usr/lib/jvm/adoptopenjdk-8-hotspot-amd64/jre/


# --------------------------------------------------------
# SPARK
# --------------------------------------------------------

ENV SPARK_VERSION=spark-3.1.3-bin-hadoop3.2
ENV SPARK_URL https://www.apache.org/dist/spark/spark-3.1.3/${SPARK_VERSION}.tgz
ENV SPARK_HOME=/opt/$SPARK_VERSION
ENV PATH $SPARK_HOME/bin:$PATH
ENV HADOOP_CONF_DIR=$SPARK_HOME/conf
ENV PYSPARK_PYTHON=python3
ENV PYTHONHASHSEED=1

RUN set -x \
    && curl -fSL "${SPARK_URL}" -o /tmp/spark.tar.gz \
    && tar -xvzf /tmp/spark.tar.gz -C /opt/ \
    && rm /tmp/spark.tar.gz*

COPY setup.cfg ${AIRFLOW_HOME}/astronomer_providers/setup.cfg
COPY pyproject.toml ${AIRFLOW_HOME}/astronomer_providers/pyproject.toml

RUN pip install -e "${AIRFLOW_HOME}/astronomer_providers[all,tests,mypy]"
USER astro
