# Remove this in release 2.0.0
import warnings
from typing import Any

from astronomer.providers.cncf.kubernetes.hooks.kubernetes import (
    KubernetesHookAsync as KubernetesHook,
)

# This requires to show warning in code editor
warnings.warn(
    "This module is deprecated. Please use `astronomer.providers.cncf.kubernetes.hooks.kubernetes`.",
    DeprecationWarning,
    stacklevel=2,
)


class KubernetesHookAsync(KubernetesHook):
    """This module is deprecated. Please use `astronomer.providers.cncf.kubernetes.hooks.kubernetes`."""

    def __init__(self, **kwargs: Any):
        warnings.warn(
            "This module is deprecated. Please use `astronomer.providers.cncf.kubernetes.hooks.kubernetes`.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(**kwargs)
