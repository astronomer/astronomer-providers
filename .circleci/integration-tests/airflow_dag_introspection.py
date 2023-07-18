import time

from airflow.models.taskinstance import TaskInstance
from airflow.utils.log.log_reader import TaskLogReader
from airflow.utils.session import create_session
from sqlalchemy.orm import joinedload


def check_log_retries(max_retries: int, log_container: list):
    """Fetch task logs with retry."""
    retries = 0
    while retries < max_retries:
        try:
            if log_container and log_container[0] and log_container[0][0]:
                logs = log_container[0][0][1]
                break
            else:
                print("Invalid log_container structure. Unable to retrieve logs.")
                retries += 1
        except IndexError:
            print("IndexError occurred. Retrying...")
            retries += 1
        except Exception as e:
            print(f"An exception occurred: {e}")
            if retries < max_retries:
                # Wait for some time before retrying
                time.sleep(60)
                print("Retrying...")
                # Increment the number of retries
                retries += 1
            else:
                print("Maximum number of retries reached.")
    return logs


def check_log(ti_id: str, expected: str, notexpected: str, try_number: int = 1, **context: dict):
    """Get Task logs of a given TI, also validate expected substring,
    we can also provide a substring which is not expected
    """
    time.sleep(30)
    dagrun = context["dag_run"]
    task_instances = dagrun.get_task_instances()
    this_task_instance = next(filter(lambda ti: ti.task_id == ti_id, task_instances))  # ti_call

    def check(ti, expect: str, notexpect: str):
        with create_session() as session:
            ti = (
                session.query(TaskInstance)
                .filter(
                    TaskInstance.task_id == ti.task_id,
                    TaskInstance.dag_id == ti.dag_id,
                    TaskInstance.run_id == ti.run_id,
                    TaskInstance.map_index == ti.map_index,
                )
                .join(TaskInstance.dag_run)
                .options(joinedload("trigger"))
                .options(joinedload("trigger.triggerer_job"))
            ).first()
        task_log_reader = TaskLogReader()

        log_container, _ = task_log_reader.read_log_chunks(ti, try_number, {"download_logs": True})
        logs = check_log_retries(10, log_container)
        print(f"Found logs: {logs}")
        if notexpect in logs:
            raise Exception(f"{notexpect}  is present in the 'logs'.")
        if expect not in logs:
            raise Exception(f"{notexpect} is not present in the 'logs'.")
        print(f"Found {expect} but not {notexpect}")

    check(this_task_instance, expected, notexpected)
