from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor

from astronomer.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensorAsync

PROJECT_ID = "test-astronomer-airflow-providers"
DATASET_NAME = "test-astro_dataset"
TABLE_NAME = "test-partitioned_table"


class TestBigQueryTableExistenceSensorAsync:
    def test_init(self):
        task = BigQueryTableExistenceSensorAsync(
            task_id="bq_check_table",
            project_id=PROJECT_ID,
            dataset_id=DATASET_NAME,
            table_id=TABLE_NAME,
        )
        assert isinstance(task, BigQueryTableExistenceSensor)
        assert task.deferrable is True
