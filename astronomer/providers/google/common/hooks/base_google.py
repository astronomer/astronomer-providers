from typing import Any

from airflow.hooks.base import BaseHook
from asgiref.sync import sync_to_async


class GoogleBaseHookAsync(BaseHook):
    """GoogleBaseHookAsync inherits from BaseHook class, run on the trigger worker"""

    sync_hook_class: Any = None

    def __init__(self, **kwargs: Any):
        self._hook_kwargs = kwargs
        self._sync_hook = None

    async def get_sync_hook(self) -> Any:
        """
        Sync version of the Google Cloud Hooks makes blocking calls in ``__init__`` so we don't inherit
        from it.
        """
        if not self._sync_hook:
            self._sync_hook = await sync_to_async(self.sync_hook_class)(**self._hook_kwargs)
        return self._sync_hook

    async def service_file_as_context(self) -> Any:  # noqa: D102
        sync_hook = await self.get_sync_hook()
        return await sync_to_async(sync_hook.provide_gcp_credential_file_as_context)()
