import asyncio

import pytest

from astronomer_operators.amazon.aws.hooks.base_aws_async import AwsBaseHookAsync


@pytest.mark.asyncio
async def test_aws_base_hook_async_get_client_async():
    aws_base_hook_async_obj = AwsBaseHookAsync(client_type="S3", resource_type="S3")
    task = asyncio.create_task(aws_base_hook_async_obj.get_client_async())

    await asyncio.sleep(0.5)
    assert task.done() is True
