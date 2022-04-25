FROM quay.io/astronomer/ap-airflow:2.2.5 as staging

ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False

USER root

RUN apt-get update -y \
    && apt-get install -y software-properties-common \
    && apt-get install -y wget procps gnupg2 \
    && wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | sudo apt-key add -

RUN add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/de
RUN apt-get update -y && apt-get install -y adoptopenjdk-8-hotspot
ENV JAVA_HOME=/usr/lib/jvm/adoptopenjdk-8-hotspot-amd64

RUN apt-get update -y \
    && apt-get install -y git \
    && apt-get install -y unzip \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libsasl2-2 \
        libsasl2-dev \
        libsasl2-modules \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# install AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install

# install eksctl
RUN curl --silent --location "https://github.com/weaveworks/eksctl/releases/latest/download/eksctl_$(uname -s)_amd64.tar.gz" | tar xz -C /tmp \
    && mv /tmp/eksctl /usr/local/bin

# install kubectl
RUN curl -o kubectl https://s3.us-west-2.amazonaws.com/amazon-eks/1.22.6/2022-03-09/bin/linux/amd64/kubectl \
    && chmod +x ./kubectl \
    && mv ./kubectl /usr/local/bin

#install Apache Hive 2.3.9 (Download same version of library to match EMR hive version created by DAG)
RUN curl "https://downloads.apache.org/hive/hive-2.3.9/apache-hive-2.3.9-bin.tar.gz" -o  "/tmp/apache-hive-2.3.9-bin.tar.gz"  \
    && tar -xf /tmp/apache-hive-2.3.9-bin.tar.gz -C /usr/local
ENV HIVE_HOME=/usr/local/apache-hive-2.3.9-bin

#install Apache Hadoop 2.10.1 (Download same version of library to match EMR hadopp version created by DAG)
RUN curl "https://archive.apache.org/dist/hadoop/common/hadoop-2.10.1/hadoop-2.10.1.tar.gz" -o "/tmp/hadoop-2.10.1.tar.gz" \
    && tar -xf /tmp/hadoop-2.10.1.tar.gz -C /usr/local/
ENV HADOOP_HOME=/usr/local/hadoop-2.10.1
ENV HADOOP_CONF_DIR=$HADOOP_HOME/conf

RUN curl "https://raw.githubusercontent.com/apache/hive/master/data/conf/hive-site.xml" -o "/tmp/hive-site.xml" \
    && mv /tmp/hive-site.xml $HADOOP_HOME/conf

ENV PATH $PATH:$JAVA_HOME/bin:$HIVE_HOME:$HIVE_HOME/bin:$HADOOP_HOME:$HADOOP_HOME/bin:$SPARK_HOME:$SPARK_HOME/bin:$HADOOP_CONF_DIR

COPY astronomer-providers /tmp/astronomer-providers
RUN pip install /tmp/astronomer-providers[all]
RUN pip install apache-airflow[slack]

RUN mkdir -p ${AIRFLOW_HOME}/dags

COPY . .
RUN cp -r example_* ${AIRFLOW_HOME}/dags
RUN cp master_dag.py  ${AIRFLOW_HOME}/dags/

USER astro

# Deploy from astro runtime image
FROM quay.io/astronomer/astro-runtime:4.2.4-base as astro-cloud

ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False

USER root

RUN apt-get update -y \
    && apt-get install -y software-properties-common \
    && apt-get install -y wget procps gnupg2 \
    && wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | sudo apt-key add -

RUN add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/
RUN apt-get update && apt-get install -y adoptopenjdk-8-hotspot
ENV JAVA_HOME=/usr/lib/jvm/adoptopenjdk-8-hotspot-amd64

RUN apt-get update -y \
    && apt-get install -y git \
    && apt-get install -y unzip \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libsasl2-2 \
        libsasl2-dev \
        libsasl2-modules \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# install AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install

# install eksctl
RUN curl --silent --location "https://github.com/weaveworks/eksctl/releases/latest/download/eksctl_$(uname -s)_amd64.tar.gz" | tar xz -C /tmp \
    && mv /tmp/eksctl /usr/local/bin

# install kubectl
RUN curl -o kubectl https://s3.us-west-2.amazonaws.com/amazon-eks/1.22.6/2022-03-09/bin/linux/amd64/kubectl \
    && chmod +x ./kubectl \
    && mv ./kubectl /usr/local/bin

#install Apache Hive 2.3.9 (Download same version of library to match EMR hive version created by DAG)
RUN curl "https://downloads.apache.org/hive/hive-2.3.9/apache-hive-2.3.9-bin.tar.gz" -o  "/tmp/apache-hive-2.3.9-bin.tar.gz"  \
    && tar -xf /tmp/apache-hive-2.3.9-bin.tar.gz -C /usr/local
ENV HIVE_HOME=/usr/local/apache-hive-2.3.9-bin

#install Apache Hadoop 2.10.1 (Download same version of library to match EMR hadopp version created by DAG)
RUN curl "https://archive.apache.org/dist/hadoop/common/hadoop-2.10.1/hadoop-2.10.1.tar.gz" -o "/tmp/hadoop-2.10.1.tar.gz" \
    && tar -xf /tmp/hadoop-2.10.1.tar.gz -C /usr/local/
ENV HADOOP_HOME=/usr/local/hadoop-2.10.1
ENV HADOOP_CONF_DIR=$HADOOP_HOME/conf

RUN curl "https://raw.githubusercontent.com/apache/hive/master/data/conf/hive-site.xml" -o "/tmp/hive-site.xml" \
    && mv /tmp/hive-site.xml $HADOOP_HOME/conf

ENV PATH $PATH:$JAVA_HOME/bin:$HIVE_HOME:$HIVE_HOME/bin:$HADOOP_HOME:$HADOOP_HOME/bin:$SPARK_HOME:$SPARK_HOME/bin:$HADOOP_CONF_DIR

COPY astronomer-providers /tmp/astronomer-providers
RUN pip install /tmp/astronomer-providers[all]
RUN pip install apache-airflow[slack]


RUN mkdir -p ${AIRFLOW_HOME}/dags
COPY . .
RUN cp -r example_* ${AIRFLOW_HOME}/dags
RUN cp master_dag.py  ${AIRFLOW_HOME}/dags/

USER astro
