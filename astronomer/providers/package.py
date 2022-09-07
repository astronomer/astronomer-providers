import configparser
from pathlib import Path

config = configparser.ConfigParser(strict=False)
_REPO_DIR = Path(__file__).parent.parent.parent
config.read(_REPO_DIR / "setup.cfg")


def get_provider_info() -> dict:
    """Return provider metadata to Airflow"""
    return {
        # Required.
        "package-name": "astronomer-providers",
        "name": "Astro SQL Provider",
        "description": config["metadata"]["description"],
        "versions": [config["metadata"]["version"]],
        # Optional.
        "hook-class-names": [],
        "extra-links": [],
    }
