from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryGetDataOperator,
    BigQueryInsertJobOperator,
    BigQueryIntervalCheckOperator,
    BigQueryValueCheckOperator,
)

from astronomer.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperatorAsync,
    BigQueryGetDataOperatorAsync,
    BigQueryInsertJobOperatorAsync,
    BigQueryIntervalCheckOperatorAsync,
    BigQueryValueCheckOperatorAsync,
)

TEST_DATASET_LOCATION = "EU"
TEST_GCP_PROJECT_ID = "test-project"
TEST_DATASET = "test-dataset"
TEST_TABLE = "test-table"


class TestBigQueryInsertJobOperatorAsync:
    def test_init(self):
        job_id = "123456"

        configuration = {
            "query": {
                "query": "SELECT * FROM any",
                "useLegacySql": False,
            }
        }
        task = BigQueryInsertJobOperatorAsync(
            task_id="insert_query_job",
            configuration=configuration,
            location=TEST_DATASET_LOCATION,
            job_id=job_id,
            project_id=TEST_GCP_PROJECT_ID,
        )
        assert isinstance(task, BigQueryInsertJobOperator)
        assert task.deferrable is True


class TestBigQueryCheckOperatorAsync:
    def test_init(self):
        task = BigQueryCheckOperatorAsync(
            task_id="bq_check_operator_job",
            sql="SELECT * FROM any",
            location=TEST_DATASET_LOCATION,
        )
        assert isinstance(task, BigQueryCheckOperator)
        assert task.deferrable is True


class TestBigQueryIntervalCheckOperatorAsync:
    def test_init(self):
        task = BigQueryIntervalCheckOperatorAsync(
            task_id="bq_interval_check_operator_execute_complete",
            table="test_table",
            metrics_thresholds={"COUNT(*)": 1.5},
            location=TEST_DATASET_LOCATION,
        )
        assert isinstance(task, BigQueryIntervalCheckOperator)
        assert task.deferrable is True


class TestBigQueryGetDataOperatorAsync:
    def test_init(self):
        task = BigQueryGetDataOperatorAsync(
            task_id="get_data_from_bq",
            dataset_id=TEST_DATASET,
            table_id=TEST_TABLE,
            max_results=100,
            selected_fields="value,name",
        )
        assert isinstance(task, BigQueryGetDataOperator)
        assert task.deferrable is True


class TestBigQueryValueCheckOperatorAsync:
    def test_init(self):
        query = "SELECT COUNT(*) FROM Any"
        pass_val = 2
        task = BigQueryValueCheckOperatorAsync(
            task_id="check_value",
            sql=query,
            pass_value=pass_val,
            use_legacy_sql=True,
        )
        assert isinstance(task, BigQueryValueCheckOperator)
        assert task.deferrable is True
