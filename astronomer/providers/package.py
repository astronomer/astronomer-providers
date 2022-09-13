import configparser
from pathlib import Path
from typing import Any, Dict

config = configparser.ConfigParser(strict=False)
_REPO_DIR = Path(__file__).parent.parent.parent
config.read(_REPO_DIR / "setup.cfg")
description = config.get("metadata", "description", fallback="")
description = f"`{description} <https://github.com/astronomer/astronomer-providers/>`__"


def get_provider_info() -> Dict[str, Any]:
    """Return provider metadata to Airflow"""
    return {
        # Required.
        "package-name": "astronomer-providers",
        "name": "Astronomer Providers",
        "description": (description),
        "versions": [config.get("metadata", "version", fallback="")],
        # Optional.
        "hook-class-names": [],
        "extra-links": [],
    }
