from datetime import datetime
from unittest import mock

import pytest
from airflow.models import DAG, TaskInstance
from airflow.models.dagrun import DagRun
from airflow.utils.dates import days_ago
from airflow.utils.types import DagRunType

from astronomer.providers.amazon.aws.extractors.redshift import RedshiftAsyncExtractor
from astronomer.providers.amazon.aws.operators.redshift_data import (
    RedshiftDataOperatorAsync,
)

REDSHIFT_DATABASE = "dev"

REDSHIFT_QUERY = """
CREATE TABLE IF NOT EXISTS fruit (
            fruit_id INTEGER,
            name VARCHAR NOT NULL,
            color VARCHAR NOT NULL
            );
            """
DAG_ID = "email_discounts"
DAG_OWNER = "datascience"
DAG_DEFAULT_ARGS = {
    "owner": DAG_OWNER,
    "depends_on_past": False,
    "start_date": days_ago(7),
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["datascience@example.com"],
}


@pytest.fixture
def context():
    """
    Creates an empty context.
    """
    context = {"ti": {"xcom_push": {}}}
    yield context


def create_context(task):
    dag = DAG(dag_id=DAG_ID)
    logical_date = datetime(2022, 1, 1, 0, 0, 0)
    dag_run = DagRun(
        dag_id=dag.dag_id,
        execution_date=logical_date,
        run_id=DagRun.generate_run_id(DagRunType.MANUAL, logical_date),
    )
    task_instance = TaskInstance(task=task)
    task_instance.dag_run = dag_run
    task_instance.dag_id = dag.dag_id
    task_instance.xcom_push = mock.MagicMock(key="return_value", value=["123"])
    return {
        "dag": dag,
        "run_id": dag_run.run_id,
        "task": task,
        "ti": task_instance,
        "task_instance": task_instance,
        "logical_date": logical_date,
    }


TASK_ID = "select"
TASK = RedshiftDataOperatorAsync(
    task_id=TASK_ID,
    sql=REDSHIFT_QUERY,
    database=REDSHIFT_DATABASE,
)


def test_get_operator_classnames():
    extractor = RedshiftAsyncExtractor(TASK)
    class_name = extractor.get_operator_classnames()
    assert class_name == ["RedshiftDataOperatorAsync", "RedshiftSQLOperatorAsync"]


@mock.patch("airflow.models.taskinstance.TaskInstance.xcom_pull")
def test__get_xcom_redshift_job_id_none(mock_xcom_pull):
    mock_xcom_pull.return_value = {}
    extractor = RedshiftAsyncExtractor(TASK)
    redshift_job_id = extractor._get_xcom_redshift_job_id(mock_xcom_pull)
    assert redshift_job_id is None


@mock.patch("airflow.models.taskinstance.TaskInstance")
def test__get_xcom_redshift_job_id(mock_task_instance):
    mock_task_instance.xcom_pull.return_value = ["123"]
    extractor = RedshiftAsyncExtractor(TASK)
    redshift_job_id = extractor._get_xcom_redshift_job_id(mock_task_instance)
    assert "123" == redshift_job_id
