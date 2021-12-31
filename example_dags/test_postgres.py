from airflow import DAG
from airflow.utils.dates import days_ago

from astronomer_operators.postgres.operators.postgres import PostgresOperatorAsync

# Ensure to create a postgres_default connection before triggering this DAG.

# create_pet_table, populate_pet_table, get_all_pets, and get_birth_date are examples of tasks created by
# instantiating the Postgres Async Operator

with DAG(
    dag_id="postgres_async_dag",
    start_date=days_ago(0),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    create_pet_table = PostgresOperatorAsync(
        task_id="create_pet_table",
        sql="""
            CREATE TABLE IF NOT EXISTS pet (
            pet_id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL,
            pet_type VARCHAR NOT NULL,
            birth_date DATE NOT NULL,
            OWNER VARCHAR NOT NULL);
          """,
    )

    populate_pet_table = PostgresOperatorAsync(
        task_id="populate_pet_table",
        sql="""
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Max', 'Dog', '2018-07-05', 'Jane');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Susie', 'Cat', '2019-05-01', 'Phil');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Lester', 'Hamster', '2020-06-23', 'Lily');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Quincy', 'Parrot', '2013-08-11', 'Anne');
            """,
    )

    get_all_pets = PostgresOperatorAsync(task_id="get_all_pets", sql="SELECT * FROM pet;")
    get_birth_date = PostgresOperatorAsync(
        task_id="get_birth_date",
        sql="""
            SELECT * FROM pet
            WHERE birth_date
            BETWEEN SYMMETRIC DATE '{{ params.begin_date }}' AND DATE '{{ params.end_date }}'
            """,
        params={"begin_date": "2020-01-01", "end_date": "2020-12-31"},
    )

    test_pg = PostgresOperatorAsync(task_id="test_pg", sql="SELECT pg_sleep(2); SELECT 42")

    create_pet_table >> populate_pet_table >> get_all_pets >> get_birth_date
    create_pet_table >> test_pg
