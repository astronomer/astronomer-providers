FROM quay.io/astronomer/ap-airflow:2.2.4

ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False

USER root
RUN apt-get update -y \
    && apt-get install -y git \
    && apt-get install -y unzip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# install AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN ./aws/install

# install eksctl
RUN curl --silent --location "https://github.com/weaveworks/eksctl/releases/latest/download/eksctl_$(uname -s)_amd64.tar.gz" | tar xz -C /tmp
RUN sudo mv /tmp/eksctl /usr/local/bin
RUN eksctl version

# install kubectl
RUN curl -o kubectl https://s3.us-west-2.amazonaws.com/amazon-eks/1.22.6/2022-03-09/bin/linux/amd64/kubectl

RUN chmod +x ./kubectl
RUN echo $PATH
RUN mv ./kubectl /usr/local/bin
# RUN mkdir -p $HOME/bin && cp ./kubectl $HOME/bin/kubectl && export PATH=$PATH:$HOME/bin
# RUN echo 'export PATH=$PATH:$HOME/bin' >> ~/.bashrc
RUN kubectl version --short --client

COPY astronomer-providers /tmp/astronomer-providers
RUN pip install /tmp/astronomer-providers[all]
RUN pip install apache-airflow[slack]


RUN mkdir -p ${AIRFLOW_HOME}/dags
COPY . .
RUN cp -r example_* ${AIRFLOW_HOME}/dags
RUN cp master_dag.py  ${AIRFLOW_HOME}/dags/

USER astro
