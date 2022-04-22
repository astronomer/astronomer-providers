import logging
import os
import time
from datetime import datetime, timedelta

import boto3
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from botocore.exceptions import ClientError

from astronomer.providers.amazon.aws.operators.redshift_sql import (
    RedshiftSQLOperatorAsync,
)

REDSHIFT_CONN_ID = os.environ.get("ASTRO_REDSHIFT_CONN_ID", "redshift_default")
REDSHIFT_CLUSTER_IDENTIFIER = os.environ.get("REDSHIFT_CLUSTER_IDENTIFIER", "astro-providers-cluster")
REDSHIFT_CLUSTER_MASTER_USER = os.environ.get("REDSHIFT_CLUSTER_MASTER_USER", "awsuser")
REDSHIFT_CLUSTER_MASTER_PASSWORD = os.environ.get("REDSHIFT_CLUSTER_MASTER_PASSWORD", "********")
REDSHIFT_CLUSTER_TYPE = os.environ.get("REDSHIFT_CLUSTER_TYPE", "single-node")
REDSHIFT_CLUSTER_NODE_TYPE = os.environ.get("REDSHIFT_CLUSTER_NODE_TYPE", "dc2.large")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "**********")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "***********")
AWS_DEFAULT_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-2")
REDSHIFT_CLUSTER_DB_NAME = os.environ.get("REDSHIFT_CLUSTER_DB_NAME", "astro_dev")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
}


def get_cluster_status() -> str:
    """Get the status of aws redshift cluster"""
    client = boto3.client("redshift")

    response = client.describe_clusters(
        ClusterIdentifier=REDSHIFT_CLUSTER_IDENTIFIER,
    )
    logging.info("%s", response)
    cluster = response.get("Clusters")[0]
    cluster_status: str = cluster.get("ClusterStatus")
    return cluster_status


def delete_redshift_cluster() -> None:
    """Delete a redshift cluster and wait until it completely removed"""
    client = boto3.client("redshift")

    try:
        client.delete_cluster(
            ClusterIdentifier=REDSHIFT_CLUSTER_IDENTIFIER,
            SkipFinalClusterSnapshot=True,
        )

        while True:
            if get_cluster_status() == "deleting":
                time.sleep(30)
                continue
    except ClientError:
        logging.exception("Error deleting redshift cluster")
        return None


def create_redshift_cluster() -> None:
    """Create aws redshift cluster and wait until it available"""
    client = boto3.client("redshift")

    client.create_cluster(
        DBName=REDSHIFT_CLUSTER_DB_NAME,
        ClusterIdentifier=REDSHIFT_CLUSTER_IDENTIFIER,
        ClusterType=REDSHIFT_CLUSTER_TYPE,
        NodeType=REDSHIFT_CLUSTER_NODE_TYPE,
        MasterUsername=REDSHIFT_CLUSTER_MASTER_USER,
        MasterUserPassword=REDSHIFT_CLUSTER_MASTER_PASSWORD,
        Tags=[
            {"Key": "Purpose", "Value": "ProviderTest"},
        ],
    )

    while True:
        if get_cluster_status() == "available":
            break
        time.sleep(30)


with DAG(
    dag_id="example_async_redshift_sql",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "redshift"],
) as dag:

    config = BashOperator(
        task_id="aws_config",
        bash_command=f"aws configure set aws_access_key_id {AWS_ACCESS_KEY_ID}; "
        f"aws configure set aws_secret_access_key {AWS_SECRET_ACCESS_KEY}; "
        f"aws configure set default.region {AWS_DEFAULT_REGION}; ",
    )

    create_cluster_op = PythonOperator(
        task_id="create_redshift_cluster",
        python_callable=create_redshift_cluster,
    )

    # Let use plpgsql procedure loop to defer RedshiftSQLOperatorAsync
    # since Python UDF require special permission to create and run
    # https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_FUNCTION.html
    task_create_func = RedshiftSQLOperatorAsync(
        task_id="task_create_func",
        sql="""
            create or replace procedure just_a_loop() as $$
            declare
                CurrId INTEGER := 0;
                MaxId INTEGER := 500000;
            begin
                while CurrId <= MaxId
                LOOP
                    CurrId = CurrId + 1;
                end LOOP;
            end;
            $$ language plpgsql;
            """,
        redshift_conn_id=REDSHIFT_CONN_ID,
    )

    task_long_running_query = RedshiftSQLOperatorAsync(
        task_id="task_long_running_query",
        sql="CALL just_a_loop();",
    )

    task_create_table = RedshiftSQLOperatorAsync(
        task_id="task_create_table",
        sql="""
            CREATE TABLE IF NOT EXISTS fruit (
            fruit_id INTEGER,
            name VARCHAR NOT NULL,
            color VARCHAR NOT NULL
            );
        """,
        redshift_conn_id=REDSHIFT_CONN_ID,
    )

    task_insert_data = RedshiftSQLOperatorAsync(
        task_id="task_insert_data",
        sql=[
            "INSERT INTO fruit VALUES ( 1, 'Banana', 'Yellow');",
            "INSERT INTO fruit VALUES ( 2, 'Apple', 'Red');",
            "INSERT INTO fruit VALUES ( 3, 'Lemon', 'Yellow');",
            "INSERT INTO fruit VALUES ( 4, 'Grape', 'Purple');",
            "INSERT INTO fruit VALUES ( 5, 'Pear', 'Green');",
            "INSERT INTO fruit VALUES ( 6, 'Strawberry', 'Red');",
        ],
        redshift_conn_id=REDSHIFT_CONN_ID,
    )

    task_get_all_data = RedshiftSQLOperatorAsync(
        task_id="task_get_all_data",
        sql="SELECT * FROM fruit;",
    )

    task_get_data_with_filter = RedshiftSQLOperatorAsync(
        task_id="task_get_with_filter",
        sql="SELECT * FROM fruit WHERE color = '{{ params.color }}';",
        params={"color": "Red"},
        redshift_conn_id=REDSHIFT_CONN_ID,
    )

    task_delete_table = RedshiftSQLOperatorAsync(
        task_id="task_delete_table",
        sql="drop table fruit;",
        redshift_conn_id=REDSHIFT_CONN_ID,
    )

    delete_cluster_op = PythonOperator(
        task_id="delete_redshift_cluster",
        python_callable=delete_redshift_cluster,
        trigger_rule="all_done",
    )

    end = DummyOperator(task_id="end")

    (
        config
        >> create_cluster_op
        >> task_create_func
        >> task_long_running_query
        >> task_create_table
        >> task_insert_data
        >> task_get_all_data
        >> task_get_data_with_filter
        >> task_delete_table
        >> delete_cluster_op
    )

    [task_delete_table, delete_cluster_op] >> end
