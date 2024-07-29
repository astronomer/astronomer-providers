from typing import Any, Dict


def get_provider_info() -> Dict[str, Any]:
    """Return provider metadata to Airflow"""
    return {
        # Required.
        "package-name": "astronomer-providers",
        "name": "Astronomer Providers",
        "description": "Apache Airflow Providers containing Deferrable Operators & Sensors from Astronomer",
        "versions": "1.19.2",
        # Optional.
        "connection-types": [
            {
                "hook-class-name": "astronomer.providers.core.hooks.astro.AstroHook",
                "connection-type": "Astro Cloud",
            }
        ],
        "extra-links": [],
    }
