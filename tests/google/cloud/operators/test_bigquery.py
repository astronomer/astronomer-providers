from unittest import mock
from unittest.mock import MagicMock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.models import DAG
from airflow.utils.timezone import datetime
from google.cloud.exceptions import Conflict

from astronomer.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperatorAsync,
    BigQueryGetDataOperatorAsync,
    BigQueryInsertJobOperatorAsync,
    BigQueryIntervalCheckOperatorAsync,
    BigQueryValueCheckOperatorAsync,
)
from astronomer.providers.google.cloud.triggers.bigquery import (
    BigQueryCheckTrigger,
    BigQueryGetDataTrigger,
    BigQueryInsertJobTrigger,
    BigQueryIntervalCheckTrigger,
    BigQueryValueCheckTrigger,
)

TEST_DATASET_LOCATION = "EU"
TEST_GCP_PROJECT_ID = "test-project"
TEST_DATASET = "test-dataset"
TEST_TABLE = "test-table"


@pytest.fixture
def context():
    """
    Creates an empty context.
    """
    context = {}
    yield context


@mock.patch("astronomer.providers.google.cloud.operators.bigquery._BigQueryHook")
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
@mock.patch("astronomer.providers.google.cloud.operators.bigquery._BigQueryHook")
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
@mock.patch("astronomer.providers.google.cloud.operators.bigquery._BigQueryHook")
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


@mock.patch("astronomer.providers.google.cloud.operators.bigquery._BigQueryHook")
def test_bigquery_check_operator_async(mock_hook):
    """
    Asserts that a task is deferred and a BigQueryCheckTrigger will be fired
    when the BigQueryCheckOperatorAsync is executed.
    """
    job_id = "123456"
    hash_ = "hash"
    real_job_id = f"{job_id}_{hash_}"

    mock_hook.return_value.insert_job.return_value = MagicMock(job_id=real_job_id, error_result=False)

    op = BigQueryCheckOperatorAsync(
        task_id="bq_check_operator_job",
        sql="SELECT * FROM any",
        location=TEST_DATASET_LOCATION,
    )

    with pytest.raises(TaskDeferred) as exc:
        op.execute(context)

    assert isinstance(exc.value.trigger, BigQueryCheckTrigger), "Trigger is not a BigQueryCheckTrigger"


def test_bigquery_check_operator_execute_failure(context):
    """Tests that an AirflowException is raised in case of error event"""

    operator = BigQueryCheckOperatorAsync(
        task_id="bq_check_operator_execute_failure", sql="SELECT * FROM any", location=TEST_DATASET_LOCATION
    )

    with pytest.raises(AirflowException):
        operator.execute_complete(context=None, event={"status": "error", "message": "test failure message"})


def test_bigquery_check_op_execute_complete_with_no_records():
    """Asserts that exception is raised with correct expected exception message"""

    operator = BigQueryCheckOperatorAsync(
        task_id="bq_check_operator_execute_complete", sql="SELECT * FROM any", location=TEST_DATASET_LOCATION
    )

    with pytest.raises(AirflowException) as exc:
        operator.execute_complete(context=None, event={"status": "success", "records": None})

    expected_exception_msg = "The query returned None"

    assert str(exc.value) == expected_exception_msg


def test_bigquery_check_op_execute_complete_with_non_boolean_records():
    """Executing a sql which returns a non-boolean value should raise exception"""

    test_sql = "SELECT * FROM any"

    operator = BigQueryCheckOperatorAsync(
        task_id="bq_check_operator_execute_complete", sql=test_sql, location=TEST_DATASET_LOCATION
    )

    expected_exception_msg = f"Test failed.\nQuery:\n{test_sql}\nResults:\n{[20, False]!s}"

    with pytest.raises(AirflowException) as exc:
        operator.execute_complete(context=None, event={"status": "success", "records": [20, False]})

    assert str(exc.value) == expected_exception_msg


def test_bigquery_check_operator_execute_complete():
    """Asserts that logging occurs as expected"""

    operator = BigQueryCheckOperatorAsync(
        task_id="bq_check_operator_execute_complete", sql="SELECT * FROM any", location=TEST_DATASET_LOCATION
    )

    with mock.patch.object(operator.log, "info") as mock_log_info:
        operator.execute_complete(context=None, event={"status": "success", "records": [20]})
    mock_log_info.assert_called_with("Success.")


@pytest.mark.parametrize(
    "operator_class, kwargs",
    [
        (BigQueryCheckOperatorAsync, {"sql": "Select * from test_table"}),
        (
            BigQueryIntervalCheckOperatorAsync,
            {"table": "test_table", "metrics_thresholds": {"COUNT(*)": 1.5}},
        ),
        (BigQueryValueCheckOperatorAsync, {"sql": "Select * from test_tabl", "pass_value": "Any"}),
    ],
)
def test_bigquery_conn_id_deprecation_warning(operator_class, kwargs):
    """When bigquery_conn_id is passed to the Operator, raises a warning and asserts that gcp_conn_id is used"""
    bigquery_conn_id = "google_cloud_default"
    with pytest.warns(
        DeprecationWarning,
        match=(
            "The bigquery_conn_id parameter has been deprecated. "
            "You should pass the gcp_conn_id parameter."
        ),
    ):
        operator = operator_class(
            task_id="test-bq-generic-operator", bigquery_conn_id=bigquery_conn_id, **kwargs
        )
        assert bigquery_conn_id == operator.gcp_conn_id


def test_bigquery_interval_check_operator_execute_complete():
    """Asserts that logging occurs as expected"""

    operator = BigQueryIntervalCheckOperatorAsync(
        task_id="bq_interval_check_operator_execute_complete",
        table="test_table",
        metrics_thresholds={"COUNT(*)": 1.5},
        location=TEST_DATASET_LOCATION,
    )

    with mock.patch.object(operator.log, "info") as mock_log_info:
        operator.execute_complete(context=None, event={"status": "success", "message": "Job completed"})
    mock_log_info.assert_called_with(
        "%s completed with response %s ", "bq_interval_check_operator_execute_complete", "success"
    )


def test_bigquery_interval_check_operator_execute_failure(context):
    """Tests that an AirflowException is raised in case of error event"""

    operator = BigQueryIntervalCheckOperatorAsync(
        task_id="bq_interval_check_operator_execute_complete",
        table="test_table",
        metrics_thresholds={"COUNT(*)": 1.5},
        location=TEST_DATASET_LOCATION,
    )

    with pytest.raises(AirflowException):
        operator.execute_complete(context=None, event={"status": "error", "message": "test failure message"})


@mock.patch("astronomer.providers.google.cloud.operators.bigquery._BigQueryHook")
def test_bigquery_interval_check_operator_async(mock_hook):
    """
    Asserts that a task is deferred and a BigQueryIntervalCheckTrigger will be fired
    when the BigQueryIntervalCheckOperatorAsync is executed.
    """
    job_id = "123456"
    hash_ = "hash"
    real_job_id = f"{job_id}_{hash_}"

    mock_hook.return_value.insert_job.return_value = MagicMock(job_id=real_job_id, error_result=False)

    op = BigQueryIntervalCheckOperatorAsync(
        task_id="bq_interval_check_operator_execute_complete",
        table="test_table",
        metrics_thresholds={"COUNT(*)": 1.5},
        location=TEST_DATASET_LOCATION,
    )

    with pytest.raises(TaskDeferred) as exc:
        op.execute(context)

    assert isinstance(
        exc.value.trigger, BigQueryIntervalCheckTrigger
    ), "Trigger is not a BigQueryIntervalCheckTrigger"


@mock.patch("astronomer.providers.google.cloud.operators.bigquery._BigQueryHook")
def test_bigquery_get_data_operator_async_with_selected_fields(mock_hook):
    """
    Asserts that a task is deferred and a BigQuerygetDataTrigger will be fired
    when the BigQuerygetDataOperatorAsync is executed.
    """
    job_id = "123456"
    hash_ = "hash"
    real_job_id = f"{job_id}_{hash_}"

    mock_hook.return_value.insert_job.return_value = MagicMock(job_id=real_job_id, error_result=False)

    op = BigQueryGetDataOperatorAsync(
        task_id="get_data_from_bq",
        dataset_id=TEST_DATASET,
        table_id=TEST_TABLE,
        max_results=100,
        selected_fields="value,name",
    )

    with pytest.raises(TaskDeferred) as exc:
        op.execute(context)

    assert isinstance(exc.value.trigger, BigQueryGetDataTrigger), "Trigger is not a BigQueryGetDataTrigger"


@mock.patch("astronomer.providers.google.cloud.operators.bigquery._BigQueryHook")
def test_bigquery_get_data_operator_async_without_selected_fields(mock_hook):
    """
    Asserts that a task is deferred and a BigQuerygetDataTrigger will be fired
    when the BigQuerygetDataOperatorAsync is executed.
    """
    job_id = "123456"
    hash_ = "hash"
    real_job_id = f"{job_id}_{hash_}"

    mock_hook.return_value.insert_job.return_value = MagicMock(job_id=real_job_id, error_result=False)

    op = BigQueryGetDataOperatorAsync(
        task_id="get_data_from_bq",
        dataset_id=TEST_DATASET,
        table_id=TEST_TABLE,
        max_results=100,
    )

    with pytest.raises(TaskDeferred) as exc:
        op.execute(context)

    assert isinstance(exc.value.trigger, BigQueryGetDataTrigger), "Trigger is not a BigQueryGetDataTrigger"


def test_bigquery_get_data_operator_execute_failure(context):
    """Tests that an AirflowException is raised in case of error event"""

    operator = BigQueryGetDataOperatorAsync(
        task_id="get_data_from_bq",
        dataset_id=TEST_DATASET,
        table_id="any",
        max_results=100,
    )

    with pytest.raises(AirflowException):
        operator.execute_complete(context=None, event={"status": "error", "message": "test failure message"})


def test_bigquery_get_data_op_execute_complete_with_records():
    """Asserts that exception is raised with correct expected exception message"""

    operator = BigQueryGetDataOperatorAsync(
        task_id="get_data_from_bq",
        dataset_id=TEST_DATASET,
        table_id="any",
        max_results=100,
    )

    with mock.patch.object(operator.log, "info") as mock_log_info:
        operator.execute_complete(context=None, event={"status": "success", "records": [20]})
    mock_log_info.assert_called_with("Total extracted rows: %s", 1)


def _get_value_check_async_operator(use_legacy_sql: bool = False):
    """Helper function to initialise BigQueryValueCheckOperatorAsync operator"""
    query = "SELECT COUNT(*) FROM Any"
    pass_val = 2

    return BigQueryValueCheckOperatorAsync(
        task_id="check_value",
        sql=query,
        pass_value=pass_val,
        use_legacy_sql=use_legacy_sql,
    )


@mock.patch("astronomer.providers.google.cloud.operators.bigquery._BigQueryHook")
def test_bigquery_value_check_async(mock_hook):
    """
    Asserts that a task is deferred and a BigQueryValueCheckTrigger will be fired
    when the BigQueryValueCheckOperatorAsync is executed.
    """
    operator = _get_value_check_async_operator(True)
    job_id = "123456"
    hash_ = "hash"
    real_job_id = f"{job_id}_{hash_}"
    mock_hook.return_value.insert_job.return_value = MagicMock(job_id=real_job_id, error_result=False)
    with pytest.raises(TaskDeferred) as exc:
        operator.execute(context)

    assert isinstance(
        exc.value.trigger, BigQueryValueCheckTrigger
    ), "Trigger is not a BigQueryValueCheckTrigger"


def test_bigquery_value_check_operator_execute_complete_success():
    """Tests response message in case of success event"""
    operator = _get_value_check_async_operator()

    assert (
        operator.execute_complete(context=None, event={"status": "success", "message": "Job completed!"})
        is None
    )


def test_bigquery_value_check_operator_execute_complete_failure():
    """Tests that an AirflowException is raised in case of error event"""
    operator = _get_value_check_async_operator()

    with pytest.raises(AirflowException):
        operator.execute_complete(context=None, event={"status": "error", "message": "test failure message"})


@pytest.mark.parametrize(
    "kwargs, expected",
    [
        ({"sql": "SELECT COUNT(*) from Any"}, "Argument ['pass_value'] is required"),
        ({"pass_value": "Any"}, "Argument ['sql'] is required"),
    ],
)
def test_bigquery_value_check_missing_param(kwargs, expected):
    """Assert the exception if require param not pass to BigQueryValueCheckOperatorAsync operator"""
    with pytest.raises(AirflowException) as missing_param:
        BigQueryValueCheckOperatorAsync(**kwargs)
    assert missing_param.value.args[0] == expected


def test_bigquery_value_check_empty():
    """Assert the exception if require param not pass to BigQueryValueCheckOperatorAsync operator"""
    expected, expected1 = (
        "Argument ['sql', 'pass_value'] is required",
        "Argument ['pass_value', 'sql'] is required",
    )
    with pytest.raises(AirflowException) as missing_param:
        BigQueryValueCheckOperatorAsync(kwargs={})
    assert (missing_param.value.args[0] == expected) or (missing_param.value.args[0] == expected1)
