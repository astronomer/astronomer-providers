"""Example use of SnowflakeSensorAsync."""

import os
from datetime import timedelta

from airflow import DAG
from airflow.utils.timezone import datetime

from astronomer.providers.snowflake.operators.snowflake import SnowflakeOperatorAsync
from astronomer.providers.snowflake.sensors.snowflake import SnowflakeSensorAsync

SNOWFLAKE_CONN_ID = os.getenv("ASTRO_SNOWFLAKE_CONN_ID", "snowflake_default")
SNOWFLAKE_SAMPLE_TABLE = os.getenv("SNOWFLAKE_SAMPLE_TABLE", "sample_table")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))
POKE_INTERVAL = int(os.getenv("POKE_INTERVAL", 30))
TASK_TIMEOUT = int(os.getenv("TASK_TIMEOUT", 5))


# SQL commands
CREATE_TABLE_SQL_STRING = (
    f"CREATE OR REPLACE TRANSIENT TABLE {SNOWFLAKE_SAMPLE_TABLE} (name VARCHAR(250), id INT);"
)
SQL_INSERT_STATEMENT = f"INSERT INTO {SNOWFLAKE_SAMPLE_TABLE} VALUES ('name', %(id)s)"
SNOWFLAKE_SLACK_SQL = f"SELECT name, id FROM {SNOWFLAKE_SAMPLE_TABLE};"

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "snowflake_conn_id": SNOWFLAKE_CONN_ID,
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}


with DAG(
    dag_id="example_snowflake_sensor",
    start_date=datetime(2022, 1, 1),
    schedule=None,
    default_args=default_args,
    tags=["example", "async", "snowflake"],
    catchup=False,
) as dag:
    # Creating a table and inserting data
    snowflake_op_sql_str = SnowflakeOperatorAsync(
        task_id="snowflake_op_sql_str",
        sql=CREATE_TABLE_SQL_STRING,
    )

    snowflake_op_with_params = SnowflakeOperatorAsync(
        task_id="snowflake_op_with_params",
        sql=SQL_INSERT_STATEMENT,
        parameters={"id": 56},
    )

    # [START howto_sensor_snowflake_async]
    snowflake_op_sql_sensor = SnowflakeSensorAsync(
        task_id="snowflake_op_sql_sensor",
        snowflake_conn_id="snowflake_conn",
        sql=SNOWFLAKE_SLACK_SQL,
        poke_interval=POKE_INTERVAL,
        timeout=TASK_TIMEOUT * 60,
    )
    # [END howto_sensor_snowflake_async]

    snowflake_with_callable = SnowflakeSensorAsync(
        task_id="snowflake_with_callable",
        snowflake_conn_id="snowflake_conn",
        sql=SNOWFLAKE_SLACK_SQL,
        poke_interval=POKE_INTERVAL,
        timeout=TASK_TIMEOUT * 60,
        success=lambda result: True,
    )

    snowflake_op_sql_str >> snowflake_op_with_params >> snowflake_op_sql_sensor >> snowflake_with_callable
