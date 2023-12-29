#!/bin/bash

# Check if the region parameter is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <AWS_REGION>"
    exit 1
fi

AWS_REGION="$1"

# List all EKS clusters
clusters=$(aws eks list-clusters --region $AWS_REGION | jq -r '.clusters[]')

# Loop through each EKS cluster
for cluster in $clusters; do
    echo "Processing EKS cluster: $cluster"

    # List nodegroups for the cluster
    nodegroups=$(aws eks list-nodegroups --cluster-name $cluster --region $AWS_REGION | jq -r '.nodegroups[]')

    # Delete each nodegroup
    for nodegroup in $nodegroups; do
        echo "Deleting nodegroup '$nodegroup' for cluster '$cluster'"
        aws eks delete-nodegroup --cluster-name $cluster --nodegroup-name $nodegroup --region $AWS_REGION
        aws eks wait nodegroup-deleted --cluster-name $cluster --nodegroup-name $nodegroup --region $AWS_REGION
    done


    # Delete the EKS cluster
    echo "Deleting EKS cluster: $cluster"
    aws eks delete-cluster --name $cluster --region $AWS_REGION

    # Wait for the EKS cluster to be deleted
    echo "Waiting for EKS cluster '$cluster' to be deleted..."
    aws eks wait cluster-deleted --name $cluster --region $AWS_REGION

done

echo "Script execution completed."
