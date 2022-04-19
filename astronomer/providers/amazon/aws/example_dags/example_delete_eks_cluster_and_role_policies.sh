#!/bin/bash

# cmd to delete the policy attached to the role
aws iam detach-role-policy --role-name $JOB_EXECUTION_ROLE --policy-arn arn:aws:iam::$AWS_ACCOUNT_ID:policy/$DEBUGGING_MONITORING_POLICY
aws iam detach-role-policy --role-name $JOB_EXECUTION_ROLE --policy-arn arn:aws:iam::$AWS_ACCOUNT_ID:policy/$CONTAINER_SUBMIT_JOB_POLICY
aws iam detach-role-policy --role-name $JOB_EXECUTION_ROLE --policy-arn arn:aws:iam::$AWS_ACCOUNT_ID:policy/$JOB_EXECUTION_POLICY
aws iam detach-role-policy --role-name $JOB_EXECUTION_ROLE --policy-arn arn:aws:iam::$AWS_ACCOUNT_ID:policy/$MANAGE_VIRTUAL_CLUSTERS

# cmd to delete the role attached to the cluster
aws iam delete-role --role-name $JOB_EXECUTION_ROLE

# delete the virtual cluster
aws emr-containers delete-virtual-cluster --id $VIRTUAL_CLUSTER_ID

# cmd to delete the EKS cluster and node group attached to it
eksctl delete cluster $EKS_CONTAINER_PROVIDER_CLUSTER_NAME
