#!/bin/bash

# List all OIDC providers
oidc_providers=$(aws iam list-open-id-connect-providers --output json | jq -r '.OpenIDConnectProviderList[].Arn')

# Loop through each OIDC provider
for oidc_provider in $oidc_providers
do
    # Check if the provider name contains "DO_NOT_DELETE"
    if [[ $oidc_provider == *"DO_NOT_DELETE"* ]]; then
        echo "Skipping deletion of OIDC provider: $oidc_provider"
    else
        # Delete the OIDC provider
        aws iam delete-open-id-connect-provider --open-id-connect-provider-arn "$oidc_provider"
        echo "Deleted OIDC provider: $oidc_provider"
    fi
done
