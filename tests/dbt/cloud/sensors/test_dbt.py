from airflow.providers.dbt.cloud.sensors.dbt import DbtCloudJobRunSensor

from astronomer.providers.dbt.cloud.sensors.dbt import DbtCloudJobRunSensorAsync


class TestDbtCloudJobRunSensorAsync:
    TASK_ID = "dbt_cloud_run_job"
    CONN_ID = "dbt_cloud_default"
    DBT_RUN_ID = 1234
    TIMEOUT = 300

    def test_init(self):
        task = DbtCloudJobRunSensorAsync(
            dbt_cloud_conn_id=self.CONN_ID,
            task_id=self.TASK_ID,
            run_id=self.DBT_RUN_ID,
            timeout=self.TIMEOUT,
        )

        assert isinstance(task, DbtCloudJobRunSensor)
        assert task.deferrable is True
