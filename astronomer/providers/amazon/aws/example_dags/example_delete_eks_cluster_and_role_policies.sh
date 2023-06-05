#!/bin/bash

# This script is called by the "remove_cluster_container_role_policy" task of the "example_emr_eks_pi_job" DAG.
# It performs cleanup activities for various resources launched by the "create_eks_cluster_and_role_policies.sh" script
# executed by the prior task "create_eks_cluster_kube_namespace_with_role" in the "example_emr_eks_pi_job" DAG.
# It deletes an IAM role, detaches policies from the role, removes the role from associated instance profiles,
# deletes a virtual cluster (EMR Containers), and deletes an EKS cluster and its associated node group.
# The script checks for the existence of resources before deleting them to avoid failure if they don't exist.
# It utilizes the AWS CLI to interact with IAM, EMR Containers, and EKS services.


# Check if the role exists. We do not want to fail the script if the "iam get-role command" fails when it cannot find
# the role as we want to continue with the cleanup activity. And hence, we do not apply "set -e" until after this
# command.
IAM_ROLE_EXISTS=$(aws iam get-role --role-name "$JOB_EXECUTION_ROLE" 2>&1)

# Make the script exit with the status if one of the commands fails. Without this, the Airflow task calling this script
# will be marked as 'success' and the DAG will proceed on to the subsequent tasks
set -e

# It may happen that the IAM role did not get created successfully in the create_eks_cluster_and_role_policies.sh
# script. But, the CloudFormation stack would have created the EKS cluster and node group. As we need to delete the
# EKS cluster and node group, we handle this case and continue with the cleanup activity.
if [[ $IAM_ROLE_EXISTS == *"NoSuchEntity"* ]]; then
  echo "Role '$JOB_EXECUTION_ROLE' not found. Skipping deletion."
else
  # It may happen that the role got created successfully in the create_eks_cluster_and_role_policies.sh script, but
  # policies were not attached to it. In that case, we still need to delete the role and handle this case and continue
  # with the cleanup activity. Hence, instead of detaching the policies with the known policy  names, we retrieve the
  # list of all the policies attached to the role and detach them by looping over them.
  policy_arns=$(aws iam list-attached-role-policies --role-name "$JOB_EXECUTION_ROLE" --query 'AttachedPolicies[].PolicyArn' --output text)
  # Detach each policy from the role
  for policy_arn in $policy_arns; do
    aws iam detach-role-policy --role-name "$JOB_EXECUTION_ROLE" --policy-arn "$policy_arn"
  done

  # Check if any instance profiles are associated with the role
  instance_profiles=$(aws iam list-instance-profiles-for-role --role-name "$JOB_EXECUTION_ROLE" --query 'InstanceProfiles[].InstanceProfileName' --output text)
  if [[ -n "$instance_profiles" ]]; then
    # Remove the role from each instance profile
    for profile_name in $instance_profiles; do
      aws iam remove-role-from-instance-profile --instance-profile-name "$profile_name" --role-name "$JOB_EXECUTION_ROLE"
    done
  fi

  # Delete the role
  aws iam delete-role --role-name "$JOB_EXECUTION_ROLE"
  echo "Role '$JOB_EXECUTION_ROLE' deleted."
fi


# Delete the virtual cluster. It may happen that the virtual cluster did not get created successfully in the
# create_eks_cluster_and_role_policies.sh script. Or it may have been terminated already between task retries.
# In that case, we handle these cases. And continue with deleting the EKS cluster and node group.
if [[ -n "$VIRTUAL_CLUSTER_ID" ]]; then

  # Temporarily disable the exit on error so that we can check if the virtual cluster exists or not.
  set +e
  VIRTUAL_CLUSTER_STATUS=$(aws emr-containers describe-virtual-cluster --id "$VIRTUAL_CLUSTER_ID" --query 'virtualCluster.state' 2>/dev/null)
  # Re-enable the exit on error
  set -e

  if [[ -n "$VIRTUAL_CLUSTER_STATUS" ]]; then
    if [[ "$VIRTUAL_CLUSTER_STATUS" != '"TERMINATED"' ]]; then
      aws emr-containers delete-virtual-cluster --id "$VIRTUAL_CLUSTER_ID"
      echo "Virtual cluster '$VIRTUAL_CLUSTER_ID' deleted."
    else
      echo "Virtual cluster '$VIRTUAL_CLUSTER_ID' is already terminated. Skipping deletion."
    fi
  else
    echo "Virtual cluster '$VIRTUAL_CLUSTER_ID' not found. Skipping deletion."
  fi
else
  echo "VIRTUAL_CLUSTER_ID is not set. Skipping virtual cluster deletion."
fi


# Command to delete the EKS cluster and node group attached with it. Make sure to wait for the cluster to be deleted so
# that the task reports for any potential failures.
eksctl delete cluster $EKS_CLUSTER_NAME --wait --force --timeout 60m0s
echo "EKS cluster '$EKS_CLUSTER_NAME' deleted."
