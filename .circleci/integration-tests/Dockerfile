FROM quay.io/astronomer/ap-airflow:2.2.5 as staging

ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False
ENV AWS_NUKE_VERSION=v2.17.0

USER root
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
    && ./aws/install;

RUN apt-get update && apt-get install -y wget

# install AWS nuke
RUN wget --quiet  \
    "https://github.com/rebuy-de/aws-nuke/releases/download/${AWS_NUKE_VERSION}/aws-nuke-${AWS_NUKE_VERSION}-linux-amd64.tar.gz" \
    && tar -xzvf aws-nuke-${AWS_NUKE_VERSION}-linux-amd64.tar.gz -C /usr/local/bin \
    && mv /usr/local/bin/aws-nuke-${AWS_NUKE_VERSION}-linux-amd64 /usr/local/bin/aws-nuke

# install eksctl
RUN curl --silent --location "https://github.com/weaveworks/eksctl/releases/latest/download/eksctl_$(uname -s)_amd64.tar.gz" | tar xz -C /tmp \
    && mv /tmp/eksctl /usr/local/bin

# install kubectl
RUN curl -o kubectl https://s3.us-west-2.amazonaws.com/amazon-eks/1.22.6/2022-03-09/bin/linux/amd64/kubectl \
    && chmod +x ./kubectl \
    && mv ./kubectl /usr/local/bin

COPY astronomer-providers /tmp/astronomer-providers
RUN pip install /tmp/astronomer-providers[all]
RUN pip install apache-airflow[slack]


RUN mkdir -p ${AIRFLOW_HOME}/dags
COPY . .
RUN cp -r example_* ${AIRFLOW_HOME}/dags
RUN cp master_dag.py  ${AIRFLOW_HOME}/dags/
RUN cp nuke-config.yml  ${AIRFLOW_HOME}/dags/

USER astro

# Deploy from astro runtime image
FROM quay.io/astronomer/astro-runtime:4.2.4-base as astro-cloud

ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False

USER root
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
    && ./aws/install;

RUN apt-get update && apt-get install -y wget

# install AWS nuke
RUN wget --quiet  \
    "https://github.com/rebuy-de/aws-nuke/releases/download/${AWS_NUKE_VERSION}/aws-nuke-${AWS_NUKE_VERSION}-linux-amd64.tar.gz" \
    && tar -xzvf aws-nuke-${AWS_NUKE_VERSION}-linux-amd64.tar.gz -C /usr/local/bin \
    && mv /usr/local/bin/aws-nuke-${AWS_NUKE_VERSION}-linux-amd64 /usr/local/bin/aws-nuke

# install eksctl
RUN curl --silent --location "https://github.com/weaveworks/eksctl/releases/latest/download/eksctl_$(uname -s)_amd64.tar.gz" | tar xz -C /tmp \
    && mv /tmp/eksctl /usr/local/bin

# install kubectl
RUN curl -o kubectl https://s3.us-west-2.amazonaws.com/amazon-eks/1.22.6/2022-03-09/bin/linux/amd64/kubectl \
    && chmod +x ./kubectl \
    && mv ./kubectl /usr/local/bin

COPY astronomer-providers /tmp/astronomer-providers
RUN pip install /tmp/astronomer-providers[all]
RUN pip install apache-airflow[slack]


RUN mkdir -p ${AIRFLOW_HOME}/dags
COPY . .
RUN cp -r example_* ${AIRFLOW_HOME}/dags
RUN cp master_dag.py  ${AIRFLOW_HOME}/dags/
RUN cp nuke-config.yml  ${AIRFLOW_HOME}/dags/

USER astro
