from unittest import mock
from unittest.mock import MagicMock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryGetDataOperator,
    BigQueryInsertJobOperator,
    BigQueryIntervalCheckOperator,
)

from astronomer.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperatorAsync,
    BigQueryGetDataOperatorAsync,
    BigQueryInsertJobOperatorAsync,
    BigQueryIntervalCheckOperatorAsync,
    BigQueryValueCheckOperatorAsync,
)
from astronomer.providers.google.cloud.triggers.bigquery import (
    BigQueryValueCheckTrigger,
)
from tests.utils.airflow_util import create_context

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
    def _get_value_check_async_operator(self, use_legacy_sql: bool = False):
        """Helper function to initialise BigQueryValueCheckOperatorAsync operator"""
        query = "SELECT COUNT(*) FROM Any"
        pass_val = 2

        return BigQueryValueCheckOperatorAsync(
            task_id="check_value",
            sql=query,
            pass_value=pass_val,
            use_legacy_sql=use_legacy_sql,
        )

    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryValueCheckOperator.execute")
    @mock.patch("astronomer.providers.google.cloud.operators.bigquery.BigQueryValueCheckOperatorAsync.defer")
    @mock.patch("astronomer.providers.google.cloud.operators.bigquery.BigQueryHook")
    def test_bigquery_value_check_async_finish_before_deferred(self, mock_hook, mock_defer, mock_execute):
        operator = self._get_value_check_async_operator(True)
        job_id = "123456"
        hash_ = "hash"
        real_job_id = f"{job_id}_{hash_}"
        mock_hook.return_value.insert_job.return_value = MagicMock(job_id=real_job_id, error_result=False)
        mock_hook.return_value.insert_job.return_value.running.return_value = False

        operator.execute(create_context(operator))
        assert not mock_defer.called
        assert mock_execute.called

    @mock.patch("astronomer.providers.google.cloud.operators.bigquery.BigQueryHook")
    def test_bigquery_value_check_async(self, mock_hook):
        """
        Asserts that a task is deferred and a BigQueryValueCheckTrigger will be fired
        when the BigQueryValueCheckOperatorAsync is executed.
        """
        operator = self._get_value_check_async_operator(True)
        job_id = "123456"
        hash_ = "hash"
        real_job_id = f"{job_id}_{hash_}"
        mock_hook.return_value.insert_job.return_value = MagicMock(job_id=real_job_id, error_result=False)
        with pytest.raises(TaskDeferred) as exc:
            operator.execute(create_context(operator))

        assert isinstance(
            exc.value.trigger, BigQueryValueCheckTrigger
        ), "Trigger is not a BigQueryValueCheckTrigger"

    def test_bigquery_value_check_operator_execute_complete_success(self):
        """Tests response message in case of success event"""
        operator = self._get_value_check_async_operator()

        assert (
            operator.execute_complete(context=None, event={"status": "success", "message": "Job completed!"})
            is None
        )

    def test_bigquery_value_check_operator_execute_complete_failure(self):
        """Tests that an AirflowException is raised in case of error event"""
        operator = self._get_value_check_async_operator()

        with pytest.raises(AirflowException):
            operator.execute_complete(
                context=None, event={"status": "error", "message": "test failure message"}
            )

    @pytest.mark.parametrize(
        "kwargs, expected",
        [
            ({"sql": "SELECT COUNT(*) from Any"}, "missing keyword argument 'pass_value'"),
            ({"pass_value": "Any"}, "missing keyword argument 'sql'"),
        ],
    )
    def test_bigquery_value_check_missing_param(self, kwargs, expected):
        """Assert the exception if require param not pass to BigQueryValueCheckOperatorAsync operator"""
        with pytest.raises(AirflowException) as missing_param:
            BigQueryValueCheckOperatorAsync(**kwargs)
        assert missing_param.value.args[0] == expected

    def test_bigquery_value_check_empty(self):
        """Assert the exception if require param not pass to BigQueryValueCheckOperatorAsync operator"""
        expected, expected1 = (
            "missing keyword arguments 'sql', 'pass_value'",
            "missing keyword arguments 'pass_value', 'sql'",
        )
        with pytest.raises(AirflowException) as missing_param:
            BigQueryValueCheckOperatorAsync(kwargs={})
        assert (missing_param.value.args[0] == expected) or (missing_param.value.args[0] == expected1)
