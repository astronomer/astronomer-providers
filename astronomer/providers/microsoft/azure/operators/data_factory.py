import warnings

from airflow.providers.microsoft.azure.operators.data_factory import (
    AzureDataFactoryRunPipelineOperator,
)


class AzureDataFactoryRunPipelineOperatorAsync(AzureDataFactoryRunPipelineOperator):
    """
    This class is deprecated.
    Use :class: `~airflow.providers.microsoft.azure.operators.data_factory.AzureDataFactoryRunPipelineOperator` instead
    and set `deferrable` param to `True` instead.
    """

    def __init__(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        warnings.warn(
            (
                "This class is deprecated. "
                "Use `airflow.providers.microsoft.azure.operators.data_factory.AzureDataFactoryRunPipelineOperator` "
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, deferrable=True, **kwargs)
