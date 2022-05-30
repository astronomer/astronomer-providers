import tempfile
from typing import Any, AsyncIterator, Dict, Optional, Tuple

from airflow.triggers.base import TriggerEvent

from astronomer.providers.cncf.kubernetes.triggers.wait_container import (
    WaitContainerTrigger,
)


class GKEWaitContainerTrigger(WaitContainerTrigger):
    """
    Wrapper for GKE

    :param gke_yaml_config: The GKE yaml config.
    """

    def __init__(
        self,
        *,
        gke_yaml_config: Optional[str],
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.gke_yaml_config = gke_yaml_config

    def serialize(self) -> Tuple[str, Dict[str, Any]]:  # noqa: D102
        tmp = super().serialize()
        tmp[1]["gke_yaml_config"] = self.gke_yaml_config
        return ("astronomer.providers.google.cloud.triggers.wait_container.GKEWaitContainerTrigger", tmp[1])

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]  # noqa: D102

        with tempfile.NamedTemporaryFile(mode="r+", encoding="utf-8") as file:
            file.write(self.gke_yaml_config)
            file.flush()
            file.seek(0)

            self.hook_params["config_file"] = str(file.name)
            async for event in super().run():
                yield event
