import warnings

from airflow.providers.amazon.aws.sensors.batch import BatchSensor


class BatchSensorAsync(BatchSensor):
    def __init__(self, *args, **kwargs):
        warnings.warn(
            (
                "This module is deprecated. "
                "Please use `airflow.providers.amazon.aws.sensors.batch.BatchSensor` "
                "and set deferrable to True instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        return super().__init__(*args, deferrable=True, **kwargs)
