#!/bin/bash

# What does the script do?
# This script automates the deletion of resources associated with VPCs (Virtual Private Clouds) in AWS .
# It takes a prefix as input and retrieves a list of VPC IDs with the specified prefix. Then, it iterates through each
# VPC ID and performs the following actions in the same order listed below to delete the VPC successfully.
#     1. Deletes associated subnets
#     2. First detach and then delete associated internet gateways
#     3. Disassociates subnet associations and the main route table if applicable for the route tables and then delete
#        the associated route tables
#     4. Deletes associated security groups
#     5. Deletes associated network interfaces
#     6. Finally, deletes the VPC itself
#
# Why do we need this script?
# When we create an EKS cluster using eksctl in the "emr_eks_pi_job" DAG , it creates a VPC with the specified prefix.
# When we delete the cluster as part of its cleanup script task, ideally the delete cluster command should delete the
# VPC as well. However, there were observed occurrences where the VPCs were not deleted. This script automates the
# deletion of such stale VPCs and associated resources. We call this script as a task from the AWS nuke DAG at the end
# of the master DAG run.

prefix="eksctl-providers-team-eks"

# Get the list of VPC IDs with the specified name prefix
vpc_ids=$(aws ec2 describe-vpcs --filters "Name=tag:Name,Values=$prefix*" --query "Vpcs[].VpcId" --output json | jq -r '.[]')

# Loop through each VPC ID and delete associated resources
for vpc_id in $vpc_ids; do
    echo "Deleting resources associated with VPC: $vpc_id"

    # Get the list of subnet IDs associated with the VPC
    subnet_ids=$(aws ec2 describe-subnets --filters "Name=vpc-id,Values=$vpc_id" --query "Subnets[].SubnetId" --output json | jq -r '.[]')

    # Delete subnets associated with the VPC
    for subnet_id in $subnet_ids; do
        echo "Deleting Subnet: $subnet_id"
        aws ec2 delete-subnet --subnet-id $subnet_id
    done

    # Get the list of internet gateway IDs associated with the VPC
    internet_gateway_ids=$(aws ec2 describe-internet-gateways --filters "Name=attachment.vpc-id,Values=$vpc_id" --query "InternetGateways[].InternetGatewayId" --output json | jq -r '.[]')

    # Detach and delete internet gateways associated with the VPC
    for internet_gateway_id in $internet_gateway_ids; do
        echo "Detaching Internet Gateway: $internet_gateway_id"
        aws ec2 detach-internet-gateway --internet-gateway-id $internet_gateway_id --vpc-id $vpc_id
        echo "Deleting Internet Gateway: $internet_gateway_id"
        aws ec2 delete-internet-gateway --internet-gateway-id $internet_gateway_id
    done

    # Get the list of route table IDs associated with the VPC
    route_table_ids=$(aws ec2 describe-route-tables --filters "Name=vpc-id,Values=$vpc_id" --query "RouteTables[].RouteTableId" --output json | jq -r '.[]')

    # Disassociate route tables associated with the VPC
    for route_table_id in $route_table_ids; do
        # Disassociate all subnet associations
        subnet_associations=$(aws ec2 describe-route-tables --route-table-ids $route_table_id --query "RouteTables[].Associations[?SubnetId].RouteTableAssociationId" --output json | jq -r '.[]')
        for association_id in $subnet_associations; do
            if [ -n "$association_id" ] && [ "$association_id" != "null" ] && [ "$association_id" != "[]" ]; then
                echo "Disassociating Subnet association: $association_id"
                aws ec2 disassociate-route-table --association-id $association_id
            fi
        done

        # Disassociate the main route table if applicable
        main_association=$(aws ec2 describe-route-tables --route-table-ids $route_table_id --query "RouteTables[].Associations[?Main==true].RouteTableAssociationId" --output json | jq -r '.[]')
        if [ -n "$main_association" ] && [ "$main_association" != "null" ] && [ "$main_association" != "[]" ]; then
            echo "Disassociating Main Route Table association: $main_association"
            aws ec2 disassociate-route-table --association-id $main_association
        fi

        echo "Deleting Route Table: $route_table_id"
        aws ec2 delete-route-table --route-table-id $route_table_id
    done

    # Get the list of security group IDs associated with the VPC
    security_group_ids=$(aws ec2 describe-security-groups --filters "Name=vpc-id,Values=$vpc_id" --query "SecurityGroups[].GroupId" --output json | jq -r '.[]')

    # Delete security groups associated with the VPC
    for security_group_id in $security_group_ids; do
        echo "Deleting security group: $security_group_id"
        aws ec2 delete-security-group --group-id $security_group_id
    done

    # Get the list of network interface IDs associated exclusively with the VPC
    network_interface_ids=$(aws ec2 describe-network-interfaces --filters "Name=vpc-id,Values=$vpc_id" --query "NetworkInterfaces[?Attachment.InstanceId==null].NetworkInterfaceId" --output json | jq -r '.[]')

    # Delete network interfaces associated with the VPC
    for network_interface_id in $network_interface_ids; do
        echo "Deleting Network Interface: $network_interface_id"
        aws ec2 delete-network-interface --network-interface-id $network_interface_id
    done
done

# Since all the associations would have been deleted now, delete the VPCs finally. We want to capture the exit status of
# the final commands and hence set the -e flag here.
set -e

for vpc_id in $vpc_ids; do
    echo "Deleting VPC: $vpc_id"
    aws ec2 delete-vpc --vpc-id $vpc_id
done
