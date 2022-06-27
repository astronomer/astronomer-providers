from unittest import mock
from unittest.mock import MagicMock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.models import DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils.timezone import datetime
from airflow.utils.types import DagRunType
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
        op.execute(create_context(op))

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


def create_context(task):
    dag = DAG(dag_id="dag")
    logical_date = datetime(2022, 1, 1, 0, 0, 0)
    dag_run = DagRun(
        dag_id=dag.dag_id,
        execution_date=logical_date,
        run_id=DagRun.generate_run_id(DagRunType.MANUAL, logical_date),
    )
    task_instance = TaskInstance(task=task)
    task_instance.dag_run = dag_run
    task_instance.dag_id = dag.dag_id
    task_instance.xcom_push = mock.Mock()
    return {
        "dag": dag,
        "run_id": dag_run.run_id,
        "task": task,
        "ti": task_instance,
        "task_instance": task_instance,
        "logical_date": logical_date,
    }


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
        operator.execute_complete(
            context=create_context(operator),
            event={"status": "success", "message": "Job completed", "job_id": job_id},
        )
    mock_log_info.assert_called_with("%s completed with response %s ", "insert_query_job", "Job completed")


@mock.patch("astronomer.providers.google.cloud.operators.bigquery._BigQueryHook")
def test_bigquery_insert_job_operator_with_job_id_generate(mock_hook):
    job_id = "123456"
    hash_ = "hash"
    real_job_id = f"{job_id}_{hash_}"

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
        op.execute(create_context(op))

    mock_hook.return_value.generate_job_id.assert_called_once_with(
        job_id=job_id,
        dag_id="adhoc_airflow",
        task_id="insert_query_job",
        logical_date=datetime(2022, 1, 1, 0, 0),
        configuration=configuration,
        force_rerun=True,
    )


@mock.patch("astronomer.providers.google.cloud.operators.bigquery._BigQueryHook")
def test_execute_reattach(mock_hook):
    job_id = "123456"
    hash_ = "hash"
    real_job_id = f"{job_id}_{hash_}"
    mock_hook.return_value.generate_job_id.return_value = f"{job_id}_{hash_}"

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
        op.execute(create_context(op))

    mock_hook.return_value.get_job.assert_called_once_with(
        location=TEST_DATASET_LOCATION,
        job_id=real_job_id,
        project_id=TEST_GCP_PROJECT_ID,
    )

    job._begin.assert_called_once_with()


@mock.patch("astronomer.providers.google.cloud.operators.bigquery._BigQueryHook")
def test_execute_force_rerun(mock_hook):
    job_id = "123456"
    hash_ = "hash"
    real_job_id = f"{job_id}_{hash_}"
    mock_hook.return_value.generate_job_id.return_value = f"{job_id}_{hash_}"

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
        op.execute(create_context(op))

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
        op.execute(create_context(op))

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
        op.execute(create_context(op))

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
        op.execute(create_context(op))

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
        op.execute(create_context(op))

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
        operator.execute(create_context(operator))

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
        ({"sql": "SELECT COUNT(*) from Any"}, "missing keyword argument 'pass_value'"),
        ({"pass_value": "Any"}, "missing keyword argument 'sql'"),
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
        "missing keyword arguments 'sql', 'pass_value'",
        "missing keyword arguments 'pass_value', 'sql'",
    )
    with pytest.raises(AirflowException) as missing_param:
        BigQueryValueCheckOperatorAsync(kwargs={})
    assert (missing_param.value.args[0] == expected) or (missing_param.value.args[0] == expected1)
