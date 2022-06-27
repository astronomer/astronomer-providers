# Remove this in release 2.0.0
import pytest


def test_deprecated_warning():
    with pytest.warns(expected_warning=DeprecationWarning):
        from astronomer.providers.amazon.aws.hooks.base_aws_async import (
            AwsBaseHookAsync,
        )

        AwsBaseHookAsync(client_type="S3")
