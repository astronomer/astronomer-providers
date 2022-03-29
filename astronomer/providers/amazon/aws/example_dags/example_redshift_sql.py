import os
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

from astronomer.providers.amazon.aws.operators.redshift_sql import (
    RedshiftSQLOperatorAsync,
)

REDSHIFT_CONN_ID = os.environ.get("ASTRO_REDSHIFT_CONN_ID", "redshift_default")
REDSHIFT_CLUSTER_IDENTIFIER = os.environ.get("REDSHIFT_CLUSTER_IDENTIFIER", "astro-providers-cluster")
REDSHIFT_CLUSTER_MASTER_USER = os.environ.get("REDSHIFT_CLUSTER_MASTER_USER", "adminuser")
REDSHIFT_CLUSTER_MASTER_PASSWORD = os.environ.get("REDSHIFT_CLUSTER_MASTER_PASSWORD", "********")
REDSHIFT_CLUSTER_DB_NAME = os.environ.get("REDSHIFT_CLUSTER_DB_NAME", "astro_dev")

default_args = {
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="example_async_redshift_sql",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "redshift"],
) as dag:
    # Execute AWS command then sleep for 5 min so that cluster would be available
    create_redshift_cluster = BashOperator(
        task_id="create_redshift_cluster",
        bash_command=f"aws redshift create-cluster "
        f"--db-name {REDSHIFT_CLUSTER_DB_NAME} "
        f"--cluster-identifier {REDSHIFT_CLUSTER_IDENTIFIER} "
        f"--cluster-type single-node "
        f"--node-type dc2.large  "
        f"--master-username {REDSHIFT_CLUSTER_MASTER_USER} "
        f"--master-user-password {REDSHIFT_CLUSTER_MASTER_PASSWORD} && sleep 5m",
    )

    # Let use plpgsql procedure loop to defer RedshiftSQLOperatorAsync
    # since Python UDF require special permission to create and run
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

    delete_redshift_cluster = BashOperator(
        task_id="delete_redshift_cluster",
        bash_command=f"aws redshift delete-cluster "
        f"--cluster-identifier {REDSHIFT_CLUSTER_IDENTIFIER} --skip-final-cluster-snapshot && sleep 2m",
        trigger_rule="all_done",
    )

    end = DummyOperator(task_id="end")

    (
        create_redshift_cluster
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
