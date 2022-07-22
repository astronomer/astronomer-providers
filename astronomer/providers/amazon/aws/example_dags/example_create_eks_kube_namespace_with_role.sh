#!/bin/bash

# Make the script exit with the status if one of the commands fails. Without this, the Airflow task calling this script
# will be marked as 'success' and the DAG will proceed on to the subsequent tasks.
set -e

# Create EKS cluster.
eksctl create cluster \
    --name $EKS_CLUSTER_NAME \
    --region $AWS_DEFAULT_REGION \
    --with-oidc \
    --ssh-access \
    --ssh-public-key providers_team_keypair \
    --instance-types=$EMR_INSTANCE_TYPE \
    --managed

# Create kubectl cluster namespace.
kubectl create namespace $EKS_NAMESPACE

eksctl create iamidentitymapping \
    --cluster $EKS_CLUSTER_NAME \
    --namespace $EKS_NAMESPACE \
    --service-name "emr-containers"

aws eks describe-cluster --name $EKS_CLUSTER_NAME --query "cluster.identity.oidc.issuer"

eksctl utils associate-iam-oidc-provider --cluster $EKS_CLUSTER_NAME --approve

aws iam create-role --role-name $JOB_EXECUTION_ROLE --assume-role-policy-document '{"Version": "2012-10-17","Statement":
[{"Effect": "Allow","Principal": {"AWS": "arn:aws:iam::'$AWS_ACCOUNT_ID':root"},"Action":
"sts:AssumeRole","Condition": {}}]}'


aws iam attach-role-policy --role-name $JOB_EXECUTION_ROLE --policy-arn arn:aws:iam::$AWS_ACCOUNT_ID:policy/$DEBUGGING_MONITORING_POLICY
aws iam attach-role-policy --role-name $JOB_EXECUTION_ROLE --policy-arn arn:aws:iam::$AWS_ACCOUNT_ID:policy/$CONTAINER_SUBMIT_JOB_POLICY
aws iam attach-role-policy --role-name $JOB_EXECUTION_ROLE --policy-arn arn:aws:iam::$AWS_ACCOUNT_ID:policy/$JOB_EXECUTION_POLICY
aws iam attach-role-policy --role-name $JOB_EXECUTION_ROLE --policy-arn arn:aws:iam::$AWS_ACCOUNT_ID:policy/$MANAGE_VIRTUAL_CLUSTERS


aws emr-containers update-role-trust-policy \
       --cluster-name $EKS_CLUSTER_NAME \
       --namespace $EKS_NAMESPACE \
       --role-name $JOB_EXECUTION_ROLE

export JOB_ROLE_ARN="arn:aws:iam::"$AWS_ACCOUNT_ID":role/"$JOB_EXECUTION_ROLE
