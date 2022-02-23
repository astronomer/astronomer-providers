import airflow
from airflow.utils.dates import days_ago

from astronomer.providers.amazon.aws.operators.redshift_sql import (
    RedshiftSQLOperatorAsync,
)

with airflow.DAG(
    dag_id="example_async_redshift_SQL",
    start_date=days_ago(1),
    tags=["example", "async"],
    schedule_interval="@once",
    catchup=False,
) as dag:
    setup__task_create_table = (
        RedshiftSQLOperatorAsync(
            task_id="setup__create_table",
            sql="""
            CREATE TABLE IF NOT EXISTS fruit (
            fruit_id INTEGER,
            name VARCHAR NOT NULL,
            color VARCHAR NOT NULL
            );
        """,
        ),
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
    )

    task_get_all_table_data = RedshiftSQLOperatorAsync(
        task_id="task_get_all_table_data",
        sql="CREATE TABLE more_fruit AS SELECT * FROM fruit;",
    )

    task_get_with_filter = RedshiftSQLOperatorAsync(
        task_id="task_get_with_filter",
        sql="CREATE TABLE filtered_fruit AS SELECT * FROM fruit WHERE color = '{{ params.color }}';",
        params={"color": "Red"},
    )

    setup__task_create_table >> task_insert_data >> task_get_all_table_data >> task_get_with_filter
