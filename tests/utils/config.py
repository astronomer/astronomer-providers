from airflow.utils import timezone


class Config:
    EXECUTION_DATE = timezone.datetime(2022, 1, 1)
    POLL_INTERVAL = 5
