from unittest import mock

from airflow.models import DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils import timezone


def create_context(task, dag=None):
    if dag is None:
        dag = DAG(dag_id="dag")
    logical_date = timezone.datetime(2022, 1, 1, 1, 0, 0)
    run_id = f"manual__{logical_date.isoformat()}"
    dag_run = mock.MagicMock(spec=DagRun, dag_id=dag.dag_id, run_id=run_id, logical_date=logical_date)
    task_instance = mock.MagicMock(spec=TaskInstance, task=task, dag_run=dag_run)
    return {
        "dag": dag,
        "ts": logical_date.isoformat(),
        "task": task,
        "ti": task_instance,
        "task_instance": task_instance,
        "run_id": run_id,
        "dag_run": dag_run,
        "execution_date": logical_date,
        "data_interval_end": logical_date,
        "logical_date": logical_date,
    }
