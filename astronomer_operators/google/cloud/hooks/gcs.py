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
from typing import Optional

from airflow.hooks.base import BaseHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from asgiref.sync import sync_to_async
from gcloud.aio.storage import Storage

DEFAULT_TIMEOUT = 60


class GCSHookAsync(BaseHook):
    _client = None  # type: Optional[Storage]

    def __init__(self, **kwargs):
        self._hook_kwargs = kwargs
        self._gcs_hook_sync = None

    async def get_gcs_hook_sync(self):
        """
        Sync version of the GCSHook makes blocking call in ``__init__`` so we dont
        inherit from it
        """
        if not self._gcs_hook_sync:
            self._gcs_hook_sync = await sync_to_async(GCSHook)(**self._hook_kwargs)
        return self._gcs_hook_sync

    async def service_file_as_context(self):
        sync_hook = await self.get_gcs_hook_sync()
        return await sync_to_async(sync_hook.provide_gcp_credential_file_as_context)()

    async def get_storage_client(self, session) -> Storage:
        """
        Returns a Google Cloud Storage service object.
        """
        with await self.service_file_as_context() as file:
            return Storage(service_file=file, session=session)
