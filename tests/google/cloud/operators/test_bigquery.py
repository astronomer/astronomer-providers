from unittest import mock
from unittest.mock import MagicMock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.models import DAG
from airflow.utils.timezone import datetime
from google.cloud.exceptions import Conflict

from astronomer_operators.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperatorAsync,
)
from astronomer_operators.google.cloud.triggers.bigquery import BigQueryInsertJobTrigger

TEST_DATASET_LOCATION = "EU"
TEST_GCP_PROJECT_ID = "test-project"


@pytest.fixture
def context():
    """
    Creates an empty context.
    """
    context = {}
    yield context


@mock.patch("astronomer_operators.google.cloud.operators.bigquery._BigQueryHook")
def test_bigquery_insert_job_operator_async(mock_hook):
    """
    Asserts that a task is deferred and a BigQueryInsertJobTrigger will be fired
    when the BigQueryInsertJobOperatorAsync is executed.
    """
    job_id = "123456"
    hash_ = "hash"
    real_job_id = f"{job_id}_{hash_}"

    configuration = {
        "query": {
            "query": "SELECT * FROM any",
            "useLegacySql": False,
        }
    }
    mock_hook.return_value.insert_job.return_value = MagicMock(job_id=real_job_id, error_result=False)

    op = BigQueryInsertJobOperatorAsync(
        task_id="insert_query_job",
        configuration=configuration,
        location=TEST_DATASET_LOCATION,
        job_id=job_id,
        project_id=TEST_GCP_PROJECT_ID,
    )

    with pytest.raises(TaskDeferred) as exc:
        op.execute(context)

    assert isinstance(
        exc.value.trigger, BigQueryInsertJobTrigger
    ), "Trigger is not a BigQueryInsertJobTrigger"


def test_bigquery_insert_job_operator_execute_failure(context):
    """Tests that an AirflowException is raised in case of error event"""
    configuration = {
        "query": {
            "query": "SELECT * FROM any",
            "useLegacySql": False,
        }
    }
    job_id = "123456"

    operator = BigQueryInsertJobOperatorAsync(
        task_id="insert_query_job",
        configuration=configuration,
        location=TEST_DATASET_LOCATION,
        job_id=job_id,
        project_id=TEST_GCP_PROJECT_ID,
    )

    with pytest.raises(AirflowException):
        operator.execute_complete(context=None, event={"status": "error", "message": "test failure message"})


def test_bigquery_insert_job_operator_execute_complete():
    """Asserts that logging occurs as expected"""
    configuration = {
        "query": {
            "query": "SELECT * FROM any",
            "useLegacySql": False,
        }
    }
    job_id = "123456"

    operator = BigQueryInsertJobOperatorAsync(
        task_id="insert_query_job",
        configuration=configuration,
        location=TEST_DATASET_LOCATION,
        job_id=job_id,
        project_id=TEST_GCP_PROJECT_ID,
    )
    with mock.patch.object(operator.log, "info") as mock_log_info:
        operator.execute_complete(context=None, event={"status": "success", "message": "Job completed"})
    mock_log_info.assert_called_with("%s completed with response %s ", "insert_query_job", "Job completed")


@mock.patch("airflow.providers.google.cloud.operators.bigquery.hashlib.md5")
@pytest.mark.parametrize(
    "test_dag_id, expected_job_id",
    [("test-dag-id-1.1", "airflow_test_dag_id_1_1_test_job_id_2020_01_23T00_00_00_00_00_hash")],
    ids=["test-dag-id-1.1"],
)
def test_job_id_validity(mock_md5, test_dag_id, expected_job_id):
    """Asserts that job id is correctly generated"""
    hash_ = "hash"
    mock_md5.return_value.hexdigest.return_value = hash_
    context = {"execution_date": datetime(2020, 1, 23)}
    configuration = {
        "query": {
            "query": "SELECT * FROM any",
            "useLegacySql": False,
        }
    }
    with DAG(dag_id=test_dag_id, start_date=datetime(2020, 1, 23)):
        op = BigQueryInsertJobOperatorAsync(
            task_id="test_job_id", configuration=configuration, project_id=TEST_GCP_PROJECT_ID
        )
    assert op._job_id(context) == expected_job_id


@mock.patch("airflow.providers.google.cloud.operators.bigquery.hashlib.md5")
@mock.patch("astronomer_operators.google.cloud.operators.bigquery._BigQueryHook")
def test_execute_reattach(mock_hook, mock_md5):
    job_id = "123456"
    hash_ = "hash"
    real_job_id = f"{job_id}_{hash_}"
    mock_md5.return_value.hexdigest.return_value = hash_

    configuration = {
        "query": {
            "query": "SELECT * FROM any",
            "useLegacySql": False,
        }
    }

    mock_hook.return_value.insert_job.side_effect = Conflict("any")
    job = MagicMock(
        job_id=real_job_id,
        error_result=False,
        state="PENDING",
        done=lambda: False,
    )
    mock_hook.return_value.get_job.return_value = job

    op = BigQueryInsertJobOperatorAsync(
        task_id="insert_query_job",
        configuration=configuration,
        location=TEST_DATASET_LOCATION,
        job_id=job_id,
        project_id=TEST_GCP_PROJECT_ID,
        reattach_states={"PENDING"},
    )

    with pytest.raises(TaskDeferred):
        op.execute(context)

    mock_hook.return_value.get_job.assert_called_once_with(
        location=TEST_DATASET_LOCATION,
        job_id=real_job_id,
        project_id=TEST_GCP_PROJECT_ID,
    )

    job._begin.assert_called_once_with()


@mock.patch("airflow.providers.google.cloud.operators.bigquery.hashlib.md5")
@mock.patch("astronomer_operators.google.cloud.operators.bigquery._BigQueryHook")
def test_execute_force_rerun(mock_hook, mock_md5):
    job_id = "123456"
    hash_ = "hash"
    real_job_id = f"{job_id}_{hash_}"
    mock_md5.return_value.hexdigest.return_value = hash_

    configuration = {
        "query": {
            "query": "SELECT * FROM any",
            "useLegacySql": False,
        }
    }

    mock_hook.return_value.insert_job.side_effect = Conflict("any")
    job = MagicMock(
        job_id=real_job_id,
        error_result=False,
        state="DONE",
        done=lambda: False,
    )
    mock_hook.return_value.get_job.return_value = job

    op = BigQueryInsertJobOperatorAsync(
        task_id="insert_query_job",
        configuration=configuration,
        location=TEST_DATASET_LOCATION,
        job_id=job_id,
        project_id=TEST_GCP_PROJECT_ID,
        reattach_states={"PENDING"},
    )

    with pytest.raises(AirflowException) as exc:
        op.execute(context)

    expected_exception_msg = (
        f"Job with id: {real_job_id} already exists and is in {job.state} state. "
        f"If you want to force rerun it consider setting `force_rerun=True`."
        f"Or, if you want to reattach in this scenario add {job.state} to `reattach_states`"
    )

    assert str(exc.value) == expected_exception_msg

    mock_hook.return_value.get_job.assert_called_once_with(
        location=TEST_DATASET_LOCATION,
        job_id=real_job_id,
        project_id=TEST_GCP_PROJECT_ID,
    )
