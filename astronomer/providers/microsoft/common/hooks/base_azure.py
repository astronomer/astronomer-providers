from typing import Any

from airflow.hooks.base import BaseHook
from asgiref.sync import sync_to_async


class AzureBaseHookAsync(BaseHook):
    sync_hook_class: Any = None

    def __init__(self, **kwargs):
        self._hook_kwargs = kwargs
        self._sync_hook = None

    async def get_sync_hook(self):
        """
        Sync version of the Azure Cloud Hooks makes blocking calls in ``__init__`` so we don't inherit
        from it.
        """
        if not self._sync_hook:
            self._sync_hook = await sync_to_async(self.sync_hook_class)(**self._hook_kwargs)
        return self._sync_hook
