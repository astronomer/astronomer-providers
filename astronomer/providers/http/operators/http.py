from airflow.providers.http.sensors.http import HttpSensor

from astronomer.providers.http.triggers.http import HttpTrigger


class HttpSensorAsync(HttpSensor):
    def execute(self, context):
        """
        Logic that the sensor uses to correctly identify which trigger to
        execute, and defer execution as expected.
        """
        # TODO: We can't currently serialize arbitrary function
        # Maybe we set method_name as users function??? to run it again
        # and evaluate the response.
        if self.response_check:
            super().execute(context=context)
        else:
            self.defer(
                timeout=self.execution_timeout,
                trigger=HttpTrigger(
                    method=self.hook.method,  # TODO: Fix this to directly get method from ctor
                    endpoint=self.endpoint,
                    data=self.request_params,
                    headers=self.headers,
                    extra_options=self.extra_options,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context, event=None):  # pylint: disable=unused-argument
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        self.log.info("%s completed successfully.", self.task_id)
        return None
