from typing import Any, Dict, Optional, Tuple

from astronomer.providers.cncf.kubernetes.triggers.wait_container import (
    WaitContainerTrigger,
)


class GKEWaitContainerTrigger(WaitContainerTrigger):
    """Wrapper for GKE"""

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
