#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


from unittest import mock

import pytest
from gcloud.aio.storage import Storage

from astronomer_operators.google.cloud.hooks.gcs import GCSAsyncHook


@pytest.mark.asyncio
@mock.patch("aiohttp.client.ClientSession")
async def test_get_job_status(mock_session):
    hook = GCSAsyncHook()
    result = await hook.get_storage_instance(mock_session)
    assert isinstance(result, Storage)
