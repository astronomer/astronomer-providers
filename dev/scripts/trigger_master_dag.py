import argparse
import logging
import sys

import requests

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


MASTER_DAG_ID = "example_master_dag"


def get_access_token(api_key_id: str, api_key_secret: str) -> str:
    """
    Gets bearer access token for the Astro Cloud deployment needed for REST API authentication.

    :param api_key_id: API key ID of the Astro Cloud deployment
    :param api_key_secret: API key secret of the Astro Cloud deployment
    """
    request_json = {
        "client_id": api_key_id,
        "client_secret": api_key_secret,
        "audience": "astronomer-ee",
        "grant_type": "client_credentials",
    }
    response = requests.post("https://auth.astronomer.io/oauth/token", json=request_json)
    response_json = response.json()
    return response_json["access_token"]


def trigger_master_dag_run(deployment_id: str, bearer_token: str):
    """
    Triggers the master DAG using Airflow REST API.

    :param deployment_id: ID of the Astro Cloud deployment. Using this, we generate the short deployment ID needed
        for the construction of Airflow endpoint to hit
    :param bearer_token: bearer token to be used for authentication with the Airflow REST API
    """
    short_deployment_id = f"d{deployment_id[-7:]}"
    integration_tests_deployment_url = f"https://astronomer.astronomer.run/{short_deployment_id}"
    master_dag_trigger_url = f"{integration_tests_deployment_url}/api/v1/dags/{MASTER_DAG_ID}/dagRuns"
    headers = {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "Authorization": f"Bearer {bearer_token}",
    }
    response = requests.post(master_dag_trigger_url, headers=headers, json={})
    logging.info("Response for master DAG trigger is %s", response.json())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("deployment_id", help="ID of the deployment in Astro Cloud")
    parser.add_argument("astronomer_key_id", help="Key ID of the Astro Cloud deployment")
    parser.add_argument("astronomer_key_secret", help="Key secret of the Astro Cloud deployment")
    args = parser.parse_args()
    token = get_access_token(args.astronomer_key_id.strip(), args.astronomer_key_secret.strip())
    trigger_master_dag_run(args.deployment_id, token)
