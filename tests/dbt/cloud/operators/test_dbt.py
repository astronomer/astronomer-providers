from airflow.models import DAG
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.utils import timezone

from astronomer.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperatorAsync


class TestDbtCloudRunJobOperatorAsync:
    TASK_ID = "dbt_cloud_run_job"
    CONN_ID = "dbt_cloud_default"
    DBT_RUN_ID = 1234
    CHECK_INTERVAL = 10
    TIMEOUT = 300
    DEFAULT_DATE = timezone.datetime(2021, 1, 1)
    dag = DAG("test_dbt_cloud_job_run_op", start_date=DEFAULT_DATE)

    def test_init(self):
        task = DbtCloudRunJobOperatorAsync(
            dbt_cloud_conn_id=self.CONN_ID,
            task_id=f"{self.TASK_ID}",
            job_id=self.DBT_RUN_ID,
            check_interval=self.CHECK_INTERVAL,
            timeout=self.TIMEOUT,
            dag=self.dag,
        )

        assert isinstance(task, DbtCloudRunJobOperator)
        assert task.deferrable is True
