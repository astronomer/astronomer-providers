"""Example use of SnowflakeAsync related providers."""

import os
from datetime import timedelta

from airflow import DAG
from airflow.utils.timezone import datetime

from astronomer.providers.snowflake.operators.snowflake import SnowflakeOperatorAsync

SNOWFLAKE_CONN_ID = os.getenv("ASTRO_SNOWFLAKE_CONN_ID", "snowflake_default")
SNOWFLAKE_SAMPLE_TABLE = os.getenv("SNOWFLAKE_SAMPLE_TABLE", "sample_table")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

# SQL commands
CREATE_TABLE_SQL_STRING = (
    f"CREATE OR REPLACE TRANSIENT TABLE {SNOWFLAKE_SAMPLE_TABLE} (name VARCHAR(250), id INT);"
)
SQL_INSERT_STATEMENT = f"INSERT INTO {SNOWFLAKE_SAMPLE_TABLE} VALUES ('name', %(id)s)"
SQL_LIST = [SQL_INSERT_STATEMENT % {"id": n} for n in range(0, 10)]
SQL_MULTIPLE_STMTS = "; ".join(SQL_LIST)
SNOWFLAKE_SLACK_SQL = f"SELECT name, id FROM {SNOWFLAKE_SAMPLE_TABLE} LIMIT 10;"
SNOWFLAKE_SLACK_MESSAGE = (
    "Results in an ASCII table:\n```{{ results_df | tabulate(tablefmt='pretty', headers='keys') }}```"
)

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "snowflake_conn_id": SNOWFLAKE_CONN_ID,
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}

with DAG(
    dag_id="example_snowflake",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    default_args=default_args,
    tags=["example", "async", "snowflake"],
    catchup=False,
) as dag:
    # [START howto_operator_snowflake_async]
    snowflake_op_sql_str = SnowflakeOperatorAsync(
        task_id="snowflake_op_sql_str",
        sql=CREATE_TABLE_SQL_STRING,
    )
    # [END howto_operator_snowflake_async]

    snowflake_op_with_params = SnowflakeOperatorAsync(
        task_id="snowflake_op_with_params",
        sql=SQL_INSERT_STATEMENT,
        parameters={"id": 56},
    )

    snowflake_op_sql_list = SnowflakeOperatorAsync(
        task_id="snowflake_op_sql_list",
        sql=SQL_LIST,
    )

    snowflake_op_sql_multiple_stmts = SnowflakeOperatorAsync(
        task_id="snowflake_op_sql_multiple_stmts",
        sql=SQL_MULTIPLE_STMTS,
    )

    snowflake_op_sql_select_stmts = SnowflakeOperatorAsync(
        task_id="snowflake_op_sql_select_stmts", sql=SNOWFLAKE_SLACK_SQL, return_last=False
    )

    (
        snowflake_op_sql_str
        >> [snowflake_op_with_params, snowflake_op_sql_list, snowflake_op_sql_multiple_stmts]
        >> snowflake_op_sql_select_stmts
    )
