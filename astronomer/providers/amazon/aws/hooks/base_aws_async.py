# Remove this in release 2.0.0
import warnings
from typing import Any

from astronomer.providers.amazon.aws.hooks.base_aws import AwsBaseHookAsync as AwsHook

# This requires to show warning in code editor
warnings.warn(
    "This module is deprecated. Please use `astronomer.providers.amazon.aws.hooks.base_aws`.",
    DeprecationWarning,
    stacklevel=2,
)


class AwsBaseHookAsync(AwsHook):
    """This module is deprecated. Please use `astronomer.providers.amazon.aws.hooks.base_aws`."""

    def __init__(self, *args: Any, **kwargs: Any):
        warnings.warn(
            "This module is deprecated. Please use `astronomer.providers.amazon.aws.hooks.base_aws`.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)
