FROM quay.io/astronomer/ap-airflow:2.2.5 as staging

ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False

USER root

RUN apt-get update -y \
    && apt-get install -y software-properties-common \
    && apt-get install -y wget procps gnupg2

# Install openjdk-8
RUN apt-add-repository 'deb http://security.debian.org/debian-security stretch/updates main' \
    && apt-get update && apt-get install -y openjdk-8-jdk
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/

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

# Set Hive and Hadoop versions.
ENV HIVE_LIBRARY_VERSION=hive-2.3.9
ENV HADOOP_LIBRARY_VERSION=hadoop-2.10.1

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

# Install Apache Hive (Download same version of library to match EMR Hive version created by DAG).
RUN curl "https://downloads.apache.org/hive/$HIVE_LIBRARY_VERSION/apache-$HIVE_LIBRARY_VERSION-bin.tar.gz" -o  "/tmp/apache-$HIVE_LIBRARY_VERSION-bin.tar.gz"  \
    && tar -xf /tmp/apache-$HIVE_LIBRARY_VERSION-bin.tar.gz -C /usr/local
ENV HIVE_HOME=/usr/local/apache-$HIVE_LIBRARY_VERSION-bin

# Install Apache Hadoop (Download same version of library to match EMR Hadoop version created by DAG).
RUN curl "https://archive.apache.org/dist/hadoop/common/$HADOOP_LIBRARY_VERSION/$HADOOP_LIBRARY_VERSION.tar.gz" -o "/tmp/$HADOOP_LIBRARY_VERSION.tar.gz" \
    && tar -xf /tmp/$HADOOP_LIBRARY_VERSION.tar.gz -C /usr/local
ENV HADOOP_HOME=/usr/local/$HADOOP_LIBRARY_VERSION
ENV HADOOP_CONF_DIR=$HADOOP_HOME/conf

ENV PATH $PATH:$JAVA_HOME/bin:$HIVE_HOME/bin:$HADOOP_HOME/bin

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
    && apt-get install -y wget procps gnupg2

# Install openjdk-8
RUN apt-add-repository 'deb http://security.debian.org/debian-security stretch/updates main' \
    && apt-get update && apt-get install -y openjdk-8-jdk
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/

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

# Set Hive and Hadoop versions.
ENV HIVE_LIBRARY_VERSION=hive-2.3.9
ENV HADOOP_LIBRARY_VERSION=hadoop-2.10.1

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

# Install Apache Hive (Download same version of library to match EMR Hive version created by DAG)
RUN curl "https://downloads.apache.org/hive/$HIVE_LIBRARY_VERSION/apache-$HIVE_LIBRARY_VERSION-bin.tar.gz" -o  "/tmp/apache-$HIVE_LIBRARY_VERSION-bin.tar.gz"  \
    && tar -xf /tmp/apache-$HIVE_LIBRARY_VERSION-bin.tar.gz -C /usr/local
ENV HIVE_HOME=/usr/local/apache-$HIVE_LIBRARY_VERSION-bin

# Install Apache Hadoop (Download same version of library to match EMR Hadoop version created by DAG)
RUN curl "https://archive.apache.org/dist/hadoop/common/$HADOOP_LIBRARY_VERSION/$HADOOP_LIBRARY_VERSION.tar.gz" -o "/tmp/$HADOOP_LIBRARY_VERSION.tar.gz" \
    && tar -xf /tmp/$HADOOP_LIBRARY_VERSION.tar.gz -C /usr/local
ENV HADOOP_HOME=/usr/local/$HADOOP_LIBRARY_VERSION
ENV HADOOP_CONF_DIR=$HADOOP_HOME/conf

ENV PATH $PATH:$JAVA_HOME/bin::$HIVE_HOME/bin:$HADOOP_HOME/bin

COPY astronomer-providers /tmp/astronomer-providers
RUN pip install /tmp/astronomer-providers[all]
RUN pip install apache-airflow[slack]


RUN mkdir -p ${AIRFLOW_HOME}/dags
COPY . .
RUN cp -r example_* ${AIRFLOW_HOME}/dags
RUN cp master_dag.py  ${AIRFLOW_HOME}/dags/

USER astro
