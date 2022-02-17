# Copyright 2022 Astronomer Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
from unittest import mock

import pytest
from aiobotocore.session import ClientCreatorContext
from airflow.models.connection import Connection

from astronomer_operators.amazon.aws.hooks.base_aws_async import AwsBaseHookAsync


@pytest.mark.asyncio
async def test_aws_base_hook_async_get_client_async_without_get_connection():
    aws_base_hook_async_obj = AwsBaseHookAsync(client_type="S3", resource_type="S3")
    response = await aws_base_hook_async_obj.get_client_async()

    assert isinstance(response, ClientCreatorContext)


@mock.patch("astronomer_operators.amazon.aws.hooks.base_aws_async.AwsBaseHookAsync.get_connection")
@pytest.mark.asyncio
async def test_aws_base_hook_async_get_client_async_with_get_connection(mock_connection):
    aws_base_hook_async_obj = AwsBaseHookAsync(client_type="S3", resource_type="S3")
    response = await aws_base_hook_async_obj.get_client_async()

    assert isinstance(response, ClientCreatorContext)


@mock.patch("astronomer_operators.amazon.aws.hooks.base_aws_async.AwsBaseHookAsync.get_connection")
@pytest.mark.asyncio
async def test_aws_base_hook_async_get_client_async_with_aws_secrets(mock_get_connection):
    mock_conn = Connection(extra=json.dumps({"aws_access_key_id": "", "aws_secret_access_key": ""}))
    mock_get_connection.return_value = mock_conn

    aws_base_hook_async_obj = AwsBaseHookAsync(client_type="S3", resource_type="S3")
    response = await aws_base_hook_async_obj.get_client_async()

    assert isinstance(response, ClientCreatorContext)
