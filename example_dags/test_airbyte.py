from airflow import DAG
from airflow.utils.dates import days_ago

from astronomer_operators.airbyte import AirbyteTriggerAsyncOperator

with DAG(
    "example_async_airbyte", tags=["example", "async"], start_date=days_ago(2)
) as dag:
    async_http_sensor = AirbyteTriggerAsyncOperator(
        task_id="async_airbyte",
        connection_id="e26da53d-8f89-4b35-97c5-9c79b993ff35",
        timeout=30
    )
