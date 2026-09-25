from typing import Any


def get_provider_info() -> dict[str, Any]:
    """Return provider metadata to Airflow"""
    return {
        # Required.
        "package-name": "astronomer-providers",
        "name": "Astronomer Providers",
        "description": "Apache Airflow Providers containing Deferrable Operators & Sensors from Astronomer",
        "versions": "2.0.0",
        # Optional.
        "connection-types": [
            {
                "hook-class-name": "astronomer.providers.core.hooks.astro.AstroHook",
                "connection-type": "Astro Cloud",
            }
        ],
        "extra-links": [],
    }
