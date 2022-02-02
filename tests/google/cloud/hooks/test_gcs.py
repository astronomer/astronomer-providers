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


import unittest
from unittest import mock

import pytest
from gcloud.aio.storage import Storage

from astronomer_operators.google.cloud.hooks.gcs import GCSAsyncHook

GCS_STRING = "astronomer_operators.google.cloud.hooks.gcs.{}"

PROJECT_ID_TEST = "project-id"


class TestGCSHook(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            GCS_STRING.format("GoogleBaseHook.__init__"),
        ):
            self.gcs_hook = GCSAsyncHook(gcp_conn_id="test")

    @pytest.mark.asyncio
    @mock.patch(
        "astronomer_operators.google.cloud.hooks.gcs.GoogleBaseHook.client_info",
        new_callable=mock.PropertyMock,
        return_value="CLIENT_INFO",
    )
    @mock.patch(GCS_STRING.format("GoogleBaseHook.get_connection"))
    @mock.patch("gcloud.aio.storage.Storage")
    async def test_storage_client_creation(self, mock_client, mock_client_info):
        hook = GCSAsyncHook(gcp_conn_id="test")
        result = await hook.get_conn()
        mock_client.assert_called_once_with()
        print(mock_client.assert_called_once_with())
        mock_client_info.return_value = Storage()
        assert mock_client_info.return_value == result
