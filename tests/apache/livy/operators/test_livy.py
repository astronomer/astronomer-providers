from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.apache.livy.hooks.livy import BatchState
from airflow.utils import timezone

from astronomer.providers.apache.livy.operators.livy import LivyOperatorAsync
from astronomer.providers.apache.livy.triggers.livy import LivyTrigger

DEFAULT_DATE = timezone.datetime(2017, 1, 1)
mock_livy_client = MagicMock()

BATCH_ID = 100
LOG_RESPONSE = {"total": 3, "log": ["first_line", "second_line", "third_line"]}


class TestLivyOperatorAsync:
    @pytest.fixture()
    @patch(
        "astronomer.providers.apache.livy.hooks.livy.LivyHookAsync.dump_batch_logs",
        return_value=None,
    )
    @patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync.get_batch_state")
    async def test_poll_for_termination(self, mock_livy, mock_dump_logs, dag):
        state_list = 2 * [BatchState.RUNNING] + [BatchState.SUCCESS]

        def side_effect(_, retry_args):
            if state_list:
                return state_list.pop(0)
            # fail if does not stop right before
            raise AssertionError()

        mock_livy.side_effect = side_effect

        task = LivyOperatorAsync(file="sparkapp", polling_interval=1, dag=dag, task_id="livy_example")
        task._livy_hook = task.get_hook()
        task.poll_for_termination(BATCH_ID)

        mock_livy.assert_called_with(BATCH_ID, retry_args=None)
        mock_dump_logs.assert_called_with(BATCH_ID)
        assert mock_livy.call_count == 3

    @pytest.mark.parametrize(
        "mock_state",
        (
            BatchState.NOT_STARTED,
            BatchState.STARTING,
            BatchState.RUNNING,
            BatchState.IDLE,
            BatchState.SHUTTING_DOWN,
        ),
    )
    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.post_batch", return_value=BATCH_ID)
    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.get_batch_state")
    def test_livy_operator_async(self, mock_get_batch_state, mock_post, mock_state, dag):
        mock_get_batch_state.retun_value = mock_state
        task = LivyOperatorAsync(
            livy_conn_id="livyunittest",
            file="sparkapp",
            polling_interval=1,
            dag=dag,
            task_id="livy_example",
        )

        with pytest.raises(TaskDeferred) as exc:
            task.execute({})

        assert isinstance(exc.value.trigger, LivyTrigger), "Trigger is not a LivyTrigger"

    @patch(
        "airflow.providers.apache.livy.operators.livy.LivyHook.dump_batch_logs",
        return_value=None,
    )
    @patch("astronomer.providers.apache.livy.operators.livy.LivyOperatorAsync.defer")
    @patch(
        "airflow.providers.apache.livy.operators.livy.LivyHook.get_batch", return_value={"appId": BATCH_ID}
    )
    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.post_batch", return_value=BATCH_ID)
    @patch(
        "airflow.providers.apache.livy.operators.livy.LivyHook.get_batch_state",
        return_value=BatchState.SUCCESS,
    )
    def test_livy_operator_async_finish_before_deferred_success(
        self, mock_get_batch_state, mock_post, mock_get, mock_defer, mock_dump_logs, dag
    ):
        task = LivyOperatorAsync(
            livy_conn_id="livyunittest",
            file="sparkapp",
            polling_interval=1,
            dag=dag,
            task_id="livy_example",
        )
        assert task.execute(context={"ti": MagicMock()}) == BATCH_ID
        assert not mock_defer.called

    @pytest.mark.parametrize(
        "mock_state",
        (
            BatchState.ERROR,
            BatchState.DEAD,
            BatchState.KILLED,
        ),
    )
    @patch(
        "airflow.providers.apache.livy.operators.livy.LivyHook.dump_batch_logs",
        return_value=None,
    )
    @patch("astronomer.providers.apache.livy.operators.livy.LivyOperatorAsync.defer")
    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.post_batch", return_value=BATCH_ID)
    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.get_batch_state")
    def test_livy_operator_async_finish_before_deferred_not_success(
        self, mock_get_batch_state, mock_post, mock_defer, mock_dump_logs, mock_state, dag
    ):
        mock_get_batch_state.return_value = mock_state

        task = LivyOperatorAsync(
            livy_conn_id="livyunittest",
            file="sparkapp",
            polling_interval=1,
            dag=dag,
            task_id="livy_example",
        )
        with pytest.raises(AirflowException):
            task.execute({})
        assert not mock_defer.called

    @patch(
        "airflow.providers.apache.livy.operators.livy.LivyHook.get_batch", return_value={"appId": BATCH_ID}
    )
    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.post_batch", return_value=BATCH_ID)
    def test_livy_operator_async_execute_complete_success(self, mock_post, mock_get, dag):
        """Asserts that a task is completed with success status."""
        task = LivyOperatorAsync(
            livy_conn_id="livyunittest",
            file="sparkapp",
            polling_interval=1,
            dag=dag,
            task_id="livy_example",
        )
        assert (
            task.execute_complete(
                context={"ti": MagicMock()},
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
    def test_livy_operator_async_execute_complete_error(self, mock_post, dag):
        """Asserts that a task is completed with success status."""

        task = LivyOperatorAsync(
            livy_conn_id="livyunittest",
            file="sparkapp",
            polling_interval=1,
            dag=dag,
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
