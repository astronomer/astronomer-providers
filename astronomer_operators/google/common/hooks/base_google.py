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

from typing import Any

from airflow.hooks.base import BaseHook
from asgiref.sync import sync_to_async


class GoogleBaseHookAsync(BaseHook):
    sync_hook_class: Any = None

    def __init__(self, **kwargs):
        self._hook_kwargs = kwargs
        self._sync_hook = None

    async def get_sync_hook(self):
        """
        Sync version of the Google Cloud Hooks makes blocking calls in ``__init__`` so we don't inherit
        from it.
        """
        if not self._sync_hook:
            self._sync_hook = await sync_to_async(self.sync_hook_class)(**self._hook_kwargs)
        return self._sync_hook

    async def service_file_as_context(self):
        sync_hook = await self.get_sync_hook()
        return await sync_to_async(sync_hook.provide_gcp_credential_file_as_context)()
