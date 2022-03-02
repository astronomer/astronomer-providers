import os
from datetime import datetime

from airflow.models.dag import DAG

from astronomer.providers.amazon.aws.operators.redshift_sql import (
    RedshiftSQLOperatorAsync,
)

REDSHIFT_CONN_ID = os.environ.get("ASTRO_REDSHIFT_CONN_ID", "redshift_default")

with DAG(
    dag_id="example_async_redshift_sql",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["example", "async", "redshift"],
) as dag:
    task_create_func = RedshiftSQLOperatorAsync(
        task_id="task_create_func",
        sql="""
            CREATE OR REPLACE FUNCTION janky_sleep (x float) RETURNS bool IMMUTABLE as $$
                from time import sleep
                sleep(x)
                return True
            $$ LANGUAGE plpythonu;
            """,
        redshift_conn_id=REDSHIFT_CONN_ID,
    )

    task_long_running_query_sleep = RedshiftSQLOperatorAsync(
        task_id="task_long_running_query_sleep",
        sql="select janky_sleep(10.0);",
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
            "INSERT INTO  VALUES ( 5, 'Pear', 'Green');",
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

    (
        task_create_table
        >> task_insert_data
        >> task_get_all_data
        >> task_get_data_with_filter
        >> task_delete_table
    )
