import unittest
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.providers.apache.livy.hooks.livy import BatchState
from airflow.utils import db, timezone

from astronomer.providers.apache.livy.operators.livy import LivyOperatorAsync
from astronomer.providers.apache.livy.triggers.livy import LivyTrigger

DEFAULT_DATE = timezone.datetime(2017, 1, 1)
mock_livy_client = MagicMock()

BATCH_ID = 100
LOG_RESPONSE = {"total": 3, "log": ["first_line", "second_line", "third_line"]}


class TestLivyOperatorAsync(unittest.TestCase):
    def setUp(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG("test_dag_id", default_args=args)
        db.merge_conn(
            Connection(
                conn_id="livyunittest", conn_type="livy", host="localhost:8998", port="8998", schema="http"
            )
        )

    @patch(
        "astronomer.providers.apache.livy.hooks.livy.LivyHookAsync.dump_batch_logs",
        return_value=None,
    )
    @patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync.get_batch_state")
    async def test_poll_for_termination(self, mock_livy, mock_dump_logs):
        state_list = 2 * [BatchState.RUNNING] + [BatchState.SUCCESS]

        def side_effect(_, retry_args):
            if state_list:
                return state_list.pop(0)
            # fail if does not stop right before
            raise AssertionError()

        mock_livy.side_effect = side_effect

        task = LivyOperatorAsync(file="sparkapp", polling_interval=1, dag=self.dag, task_id="livy_example")
        task._livy_hook = task.get_hook()
        task.poll_for_termination(BATCH_ID)

        mock_livy.assert_called_with(BATCH_ID, retry_args=None)
        mock_dump_logs.assert_called_with(BATCH_ID)
        assert mock_livy.call_count == 3

    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.post_batch", return_value=BATCH_ID)
    def test_livy_operator_async(self, mock_post):
        task = LivyOperatorAsync(
            livy_conn_id="livyunittest",
            file="sparkapp",
            polling_interval=1,
            dag=self.dag,
            task_id="livy_example",
        )

        with pytest.raises(TaskDeferred) as exc:
            task.execute({})

        assert isinstance(exc.value.trigger, LivyTrigger), "Trigger is not a LivyTrigger"

    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.post_batch", return_value=BATCH_ID)
    def test_livy_operator_async_execute_complete_success(self, mock_post):
        """
        Asserts that a task is completed with success status.
        """

        task = LivyOperatorAsync(
            livy_conn_id="livyunittest",
            file="sparkapp",
            polling_interval=1,
            dag=self.dag,
            task_id="livy_example",
        )
        assert (
            task.execute_complete(
                context={},
                event={
                    "status": "success",
                    "log_lines": None,
                    "batch_id": BATCH_ID,
                    "response": "mock success",
                },
            )
            is BATCH_ID
        )

    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.post_batch", return_value=BATCH_ID)
    def test_livy_operator_async_execute_complete_error(self, mock_post):
        """
        Asserts that a task is completed with success status.
        """

        task = LivyOperatorAsync(
            livy_conn_id="livyunittest",
            file="sparkapp",
            polling_interval=1,
            dag=self.dag,
            task_id="livy_example",
        )
        with pytest.raises(AirflowException):
            task.execute_complete(
                context={},
                event={
                    "status": "error",
                    "log_lines": ["mock log"],
                    "batch_id": BATCH_ID,
                    "response": "mock error",
                },
            )
