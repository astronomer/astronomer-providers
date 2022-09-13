import configparser
from pathlib import Path
from typing import Any, Dict

config = configparser.ConfigParser(strict=False)
_REPO_DIR = Path(__file__).parent.parent.parent
config.read(_REPO_DIR / "setup.cfg")
_description = config.get("metadata", "description", fallback="")
_description_link = f"`{_description} <https://github.com/astronomer/astronomer-providers/>`__"


def get_provider_info() -> Dict[str, Any]:
    """Return provider metadata to Airflow"""
    return {
        # Required.
        "package-name": "astronomer-providers",
        "name": "Astronomer Providers",
        "description": (_description_link),
        "versions": [config.get("metadata", "version", fallback="")],
        # Optional.
        "hook-class-names": [],
        "extra-links": [],
    }
