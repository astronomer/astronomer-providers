from airflow.exceptions import AirflowException
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.session import provide_session

from astronomer.providers.core.triggers.external_task import (
    DagStateTrigger,
    TaskStateTrigger,
)


class ExternalTaskSensorAsync(ExternalTaskSensor):
    def execute(self, context):
        """
        Logic that the sensor uses to correctly identify which trigger to
        execute, and defer execution as expected.
        """
        execution_dates = self.get_execution_dates(context)

        # Work out if we are a DAG sensor or a Task sensor
        # Defer to our trigger
        if not self.external_task_id:  # Tempting to explicitly check for None, but this captures falsy values
            self.defer(
                timeout=self.execution_timeout,
                trigger=DagStateTrigger(
                    dag_id=self.external_dag_id,
                    # The trigger does not do pass/fail, only "a state was reached",
                    # so we pass it all states that might make us pass or fail, and
                    # then work out which result we have in execute_complete.
                    states=self.allowed_states + self.failed_states,
                    execution_dates=execution_dates,
                ),
                method_name="execute_complete",
            )
        else:
            self.defer(
                timeout=self.execution_timeout,
                trigger=TaskStateTrigger(
                    dag_id=self.external_dag_id,
                    task_id=self.external_task_id,
                    states=self.allowed_states + self.failed_states,
                    execution_dates=execution_dates,
                ),
                method_name="execute_complete",
            )

    @provide_session
    def execute_complete(self, context, session, event=None):  # pylint: disable=unused-argument
        """
        Callback for when the trigger fires - returns immediately.
        Verifies that there is a success status for each task via execution date.
        """
        execution_dates = self.get_execution_dates(context)
        count_allowed = self.get_count(execution_dates, session, self.allowed_states)
        if count_allowed != len(execution_dates):
            if self.external_task_id:
                raise AirflowException(
                    f"The external task {self.external_task_id} in DAG {self.external_dag_id} failed."
                )
            else:
                raise AirflowException(f"The external DAG {self.external_dag_id} failed.")
        return None

    def get_execution_dates(self, context):
        """
        Helper function to set execution dates depending on which context and/or
        internal fields are populated.
        """
        if self.execution_delta:
            execution_date = context["execution_date"] - self.execution_delta
        elif self.execution_date_fn:
            execution_date = self._handle_execution_date_fn(context=context)
        else:
            execution_date = context["execution_date"]
        execution_dates = execution_date if isinstance(execution_date, list) else [execution_date]
        return execution_dates
