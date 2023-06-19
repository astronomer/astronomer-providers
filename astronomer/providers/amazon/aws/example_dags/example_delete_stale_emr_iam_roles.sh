#!/bin/bash

# What does the script do?
# This script deletes the IAM roles created by the "emr_eks_pi_job" DAG. Ideally, the "emr_eks_pi_job" DAG should delete
# the IAM roles it creates. However, there were observed occurrences where the IAM roles were not deleted, most probably
# because we did not allow to run the delete cluster task to completion or did not allow the task to run altogether
# due to killing of the deployment before it finished running. This script automates the deletion of such stale IAM roles
# by first detaching its associated policies. We call this script as a task from the AWS nuke DAG at the end of the
# master DAG run.


prefix="ProvidersTeamJobExecRole"

# Retrieve IAM roles with the specified prefix
role_names=$(aws iam list-roles --query "Roles[?starts_with(RoleName, '$prefix')].RoleName" --output json | jq -r '.[]')

for role_name in $role_names; do
    echo "Processing role: $role_name"

    # Detach all policies from the IAM role
    policy_arns=$(aws iam list-attached-role-policies --role-name "$role_name" --query "AttachedPolicies[].PolicyArn" --output json | jq -r '.[]')

    for policy_arn in $policy_arns; do
        echo "Detaching policy: $policy_arn"
        aws iam detach-role-policy --role-name "$role_name" --policy-arn "$policy_arn"
    done

    # Delete the IAM role
    echo "Deleting role: $role_name"
    aws iam delete-role --role-name "$role_name"
done
