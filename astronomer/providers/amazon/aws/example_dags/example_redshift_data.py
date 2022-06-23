import logging
import os
import time
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from astronomer.providers.amazon.aws.operators.redshift_cluster import (
    RedshiftDeleteClusterOperatorAsync,
)
from astronomer.providers.amazon.aws.operators.redshift_data import (
    RedshiftDataOperatorAsync,
)

AWS_CONN_ID = os.getenv("ASTRO_AWS_CONN_ID", "aws_default")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "**********")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "***********")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))
REDSHIFT_CLUSTER_DB_NAME = os.getenv("REDSHIFT_CLUSTER_DB_NAME", "astro_dev")
REDSHIFT_CLUSTER_IDENTIFIER = os.getenv("REDSHIFT_CLUSTER_IDENTIFIER", "astro-providers-cluster")
REDSHIFT_CLUSTER_MASTER_USER = os.getenv("REDSHIFT_CLUSTER_MASTER_USER", "awsuser")
REDSHIFT_CLUSTER_MASTER_PASSWORD = os.getenv("REDSHIFT_CLUSTER_MASTER_PASSWORD", "********")
REDSHIFT_CLUSTER_NODE_TYPE = os.getenv("REDSHIFT_CLUSTER_NODE_TYPE", "dc2.large")
REDSHIFT_CLUSTER_TYPE = os.getenv("REDSHIFT_CLUSTER_TYPE", "single-node")


default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}


def get_cluster_status() -> str:
    """Get the status of aws redshift cluster"""
    import boto3

    client = boto3.client("redshift")

    response = client.describe_clusters(
        ClusterIdentifier=REDSHIFT_CLUSTER_IDENTIFIER,
    )
    logging.info("%s", response)
    cluster = response.get("Clusters")[0]
    cluster_status: str = cluster.get("ClusterStatus")
    return cluster_status


def create_redshift_cluster_callable() -> None:
    """Creates an AWS Redshift cluster and waits until it is available."""
    import boto3

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

    while get_cluster_status() != "available":
        logging.info("Waiting for cluster to be available. Sleeping for 30 seconds.")
        time.sleep(30)


with DAG(
    dag_id="example_async_redshift_data",
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

    create_redshift_cluster = PythonOperator(
        task_id="create_redshift_cluster",
        python_callable=create_redshift_cluster_callable,
    )

    # [START howto_operator_redshift_data_operator_async]
    task_create_func = RedshiftDataOperatorAsync(
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
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
        db_user=REDSHIFT_CLUSTER_MASTER_USER,
        aws_conn_id=AWS_CONN_ID,
        database=REDSHIFT_CLUSTER_DB_NAME,
        region=AWS_DEFAULT_REGION,
    )
    # [END howto_operator_redshift_data_operator_async]

    task_long_running_query = RedshiftDataOperatorAsync(
        task_id="task_long_running_query",
        sql="CALL just_a_loop();",
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
        db_user=REDSHIFT_CLUSTER_MASTER_USER,
        aws_conn_id=AWS_CONN_ID,
        database=REDSHIFT_CLUSTER_DB_NAME,
        region=AWS_DEFAULT_REGION,
    )

    task_create_table = RedshiftDataOperatorAsync(
        task_id="task_create_table",
        sql="""
            CREATE TABLE IF NOT EXISTS fruit (
            fruit_id INTEGER,
            name VARCHAR NOT NULL,
            color VARCHAR NOT NULL
            );
        """,
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
        db_user=REDSHIFT_CLUSTER_MASTER_USER,
        aws_conn_id=AWS_CONN_ID,
        database=REDSHIFT_CLUSTER_DB_NAME,
        region=AWS_DEFAULT_REGION,
    )

    task_insert_data = RedshiftDataOperatorAsync(
        task_id="task_insert_data",
        sql=[
            "INSERT INTO fruit VALUES ( 1, 'Banana', 'Yellow');",
            "INSERT INTO fruit VALUES ( 2, 'Apple', 'Red');",
            "INSERT INTO fruit VALUES ( 3, 'Lemon', 'Yellow');",
            "INSERT INTO fruit VALUES ( 4, 'Grape', 'Purple');",
            "INSERT INTO fruit VALUES ( 5, 'Pear', 'Green');",
            "INSERT INTO fruit VALUES ( 6, 'Strawberry', 'Red');",
        ],
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
        db_user=REDSHIFT_CLUSTER_MASTER_USER,
        aws_conn_id=AWS_CONN_ID,
        database=REDSHIFT_CLUSTER_DB_NAME,
        region=AWS_DEFAULT_REGION,
    )

    task_get_all_data = RedshiftDataOperatorAsync(
        task_id="task_get_all_data",
        sql="SELECT * FROM fruit;",
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
        db_user=REDSHIFT_CLUSTER_MASTER_USER,
        aws_conn_id=AWS_CONN_ID,
        database=REDSHIFT_CLUSTER_DB_NAME,
        region=AWS_DEFAULT_REGION,
    )

    task_get_data_with_filter = RedshiftDataOperatorAsync(
        task_id="task_get_data_with_filter",
        sql="SELECT * FROM fruit WHERE color = '{{ params.color }}';",
        params={"color": "Red"},
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
        db_user=REDSHIFT_CLUSTER_MASTER_USER,
        aws_conn_id=AWS_CONN_ID,
        database=REDSHIFT_CLUSTER_DB_NAME,
        region=AWS_DEFAULT_REGION,
    )

    task_delete_table = RedshiftDataOperatorAsync(
        task_id="task_delete_table",
        sql="drop table fruit;",
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
        db_user=REDSHIFT_CLUSTER_MASTER_USER,
        aws_conn_id=AWS_CONN_ID,
        database=REDSHIFT_CLUSTER_DB_NAME,
        region=AWS_DEFAULT_REGION,
    )

    delete_redshift_cluster = RedshiftDeleteClusterOperatorAsync(
        task_id="delete_redshift_cluster",
        trigger_rule="all_done",
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
        aws_conn_id=AWS_CONN_ID,
        skip_final_cluster_snapshot=True,
        final_cluster_snapshot_identifier=None,
    )

    end = EmptyOperator(task_id="end")

    (
        config
        >> create_redshift_cluster
        >> task_create_func
        >> task_long_running_query
        >> task_create_table
        >> task_insert_data
        >> task_get_all_data
        >> task_get_data_with_filter
        >> task_delete_table
        >> delete_redshift_cluster
    )

    [task_delete_table, delete_redshift_cluster] >> end
