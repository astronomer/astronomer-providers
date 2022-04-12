#!/bin/bash

# create cluster
eksctl create cluster \
--name $EKS_CONTAINER_PROVIDER_CLUSTER_NAME \
--region us-east-2 \
--with-oidc \
--ssh-access \
--ssh-public-key providers_team_keypair \
--instance-types=m4.large \
--managed

# create kubectl cluster namespace
kubectl create namespace $KUBECTL_CLUSTER_NAME

eksctl create iamidentitymapping \
    --cluster $EKS_CONTAINER_PROVIDER_CLUSTER_NAME \
    --namespace $KUBECTL_CLUSTER_NAME \
    --service-name "emr-containers"

aws eks describe-cluster --name $EKS_CONTAINER_PROVIDER_CLUSTER_NAME --query "cluster.identity.oidc.issuer"

eksctl utils associate-iam-oidc-provider --cluster providers-team-eks-cluster --approve

aws emr-containers update-role-trust-policy \
       --cluster-name $EKS_CONTAINER_PROVIDER_CLUSTER_NAME \
       --namespace $KUBECTL_CLUSTER_NAME \
       --role-name test_job_execution_role
