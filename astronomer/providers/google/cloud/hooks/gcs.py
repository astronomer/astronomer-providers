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
#
"""This module contains a Google Cloud Storage hook."""

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from gcloud.aio.storage import Storage

from astronomer.providers.google.common.hooks.base_google import GoogleBaseHookAsync

DEFAULT_TIMEOUT = 60


class GCSHookAsync(GoogleBaseHookAsync):
    sync_hook_class = GCSHook

    async def get_storage_client(self, session) -> Storage:
        """
        Returns a Google Cloud Storage service object.
        """
        with await self.service_file_as_context() as file:
            return Storage(service_file=file, session=session)
