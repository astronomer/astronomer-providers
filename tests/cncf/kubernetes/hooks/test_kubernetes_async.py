# Remove this in release 2.0.0
import pytest


def test_deprecated_warning():
    with pytest.warns(expected_warning=DeprecationWarning):
        from astronomer.providers.cncf.kubernetes.hooks.kubernetes_async import (
            KubernetesHookAsync,
        )

        KubernetesHookAsync()
