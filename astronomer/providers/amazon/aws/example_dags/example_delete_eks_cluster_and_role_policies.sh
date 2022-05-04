#!/bin/bash

# Make the script exit with the status if one of the commands fails. Without this, the Airflow task calling this script
# will be marked as 'success' and the DAG will proceed on to the subsequent tasks.
set -e

# Command to delete the policy attached with the role.
aws iam detach-role-policy --role-name $JOB_EXECUTION_ROLE --policy-arn arn:aws:iam::$AWS_ACCOUNT_ID:policy/$DEBUGGING_MONITORING_POLICY
aws iam detach-role-policy --role-name $JOB_EXECUTION_ROLE --policy-arn arn:aws:iam::$AWS_ACCOUNT_ID:policy/$CONTAINER_SUBMIT_JOB_POLICY
aws iam detach-role-policy --role-name $JOB_EXECUTION_ROLE --policy-arn arn:aws:iam::$AWS_ACCOUNT_ID:policy/$JOB_EXECUTION_POLICY
aws iam detach-role-policy --role-name $JOB_EXECUTION_ROLE --policy-arn arn:aws:iam::$AWS_ACCOUNT_ID:policy/$MANAGE_VIRTUAL_CLUSTERS

# Command to delete the role attached with the cluster.
aws iam delete-role --role-name $JOB_EXECUTION_ROLE

# Delete the virtual cluster.
aws emr-containers delete-virtual-cluster --id $VIRTUAL_CLUSTER_ID

# Command to delete the EKS cluster and node group attached with it.
eksctl delete cluster $EKS_CLUSTER_NAME
