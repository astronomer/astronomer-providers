from __future__ import annotations

import warnings
from typing import Any

from airflow import AirflowException
from airflow.exceptions import AirflowFailException
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

from astronomer.providers.utils.typing_compat import Context


class DbtCloudRunJobOperatorAsync(DbtCloudRunJobOperator):
    """
    This class is deprecated.
    Use :class: `~airflow.providers.dbt.cloud.operators.dbt.DbtCloudRunJobOperator` instead
    and set `deferrable` param to `True` instead.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        warnings.warn(
            (
                "This class is deprecated. "
                "Use `airflow.providers.dbt.cloud.operators.dbt.DbtCloudRunJobOperator` "
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, deferrable=True, **kwargs)

    def execute_complete(self, context: Context, event: dict[str, Any]) -> int:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        # We handle the case where the job run is cancelled a bit differently than the OSS operator.
        # Essentially, we do not want to retry the task if the job run is cancelled, whereas the OSS operator will
        # retry the task if the job run is cancelled. This has been specifically handled here differently based upon
        # the feedback from a user. And hence, while we are deprecating this operator, we are not changing the behavior
        # of the `execute_complete` method. We can check if the wider OSS community wants this behavior to be changed
        # in the future as it is here, and then we can remove this override.
        if event["status"] == "cancelled":
            self.log.info("Job run %s has been cancelled.", str(event["run_id"]))
            self.log.info("Task will not be retried.")
            raise AirflowFailException(event["message"])
        elif event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info(event["message"])
        return int(event["run_id"])
