"""Example use of SnowflakeAsync related providers."""
from airflow.models.dag import DAG
from airflow.utils.timezone import datetime

from astronomer.providers.snowflake.operators.snowflake import SnowflakeOperatorAsync

SNOWFLAKE_CONN_ID = "my_snowflake_conn"
SNOWFLAKE_SAMPLE_TABLE = "sample_table"

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

# [START howto_operator_snowflake]

dag = DAG(
    "example_snowflake",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    default_args={"snowflake_conn_id": SNOWFLAKE_CONN_ID},
    tags=["example"],
    catchup=False,
)


snowflake_op_sql_str = SnowflakeOperatorAsync(
    task_id="snowflake_op_sql_str",
    dag=dag,
    sql=CREATE_TABLE_SQL_STRING,
)

snowflake_op_with_params = SnowflakeOperatorAsync(
    task_id="snowflake_op_with_params",
    dag=dag,
    sql=SQL_INSERT_STATEMENT,
    parameters={"id": 56},
)

snowflake_op_sql_list = SnowflakeOperatorAsync(task_id="snowflake_op_sql_list", dag=dag, sql=SQL_LIST)

snowflake_op_sql_multiple_stmts = SnowflakeOperatorAsync(
    task_id="snowflake_op_sql_multiple_stmts",
    dag=dag,
    sql=SQL_MULTIPLE_STMTS,
)

(snowflake_op_sql_str >> [snowflake_op_with_params, snowflake_op_sql_list, snowflake_op_sql_multiple_stmts])
