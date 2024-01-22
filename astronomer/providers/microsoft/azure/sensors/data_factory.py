import warnings
from typing import Any

from airflow.providers.microsoft.azure.sensors.data_factory import AzureDataFactoryPipelineRunStatusSensor


class AzureDataFactoryPipelineRunStatusSensorAsync(AzureDataFactoryPipelineRunStatusSensor):
    """
    This class is deprecated.
    Use :class: `~airflow.providers.microsoft.azure.sensors.data_factory.AzureDataFactoryPipelineRunStatusSensor`
    instead and set `deferrable` param to `True` instead.
    """

    def __init__(
        self,
        *args: Any,
        poll_interval: float = 5,
        **kwargs: Any,
    ):
        # TODO: Remove once deprecated
        if poll_interval:
            kwargs["poke_interval"] = poll_interval
            warnings.warn(
                "Argument `poll_interval` is deprecated and will be removed "
                "in a future release.  Please use  `poke_interval` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        super().__init__(*args, deferrable=True, **kwargs)
