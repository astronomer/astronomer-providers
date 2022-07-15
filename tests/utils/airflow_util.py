from airflow.models.baseoperator import BaseOperator
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils import timezone


def get_dag_run(dag_id: str = "test_dag_id", run_id: str = "test_dag_id") -> DagRun:
    dag_run = DagRun(
        dag_id=dag_id, run_type="manual", execution_date=timezone.datetime(2022, 1, 1), run_id=run_id
    )
    return dag_run


def get_task_instance(task: BaseOperator) -> TaskInstance:
    return TaskInstance(task, timezone.datetime(2022, 1, 1))
