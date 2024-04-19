"""This script fetches the latest runtime image tag from the provided Quay.io repository URL."""

from __future__ import annotations

import sys

import requests
from semantic_version import Version


def get_latest_tag(repository: str) -> str:
    """Get the latest semantic version tag from a Quay.io repository."""
    url = f"https://quay.io/api/v1/repository/{repository}/tag/?limit=1000"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    print("Response data:", data)
    tags = data["tags"]
    valid_tags = []
    for tag in tags:
        try:
            if not tag["name"].endswith("-base"):
                continue
            version = Version(tag["name"])
            valid_tags.append(version)
        except ValueError:
            continue
    if valid_tags:
        print(valid_tags)
        latest_tag = max(valid_tags)

       # print(latest_tag)
        return str(latest_tag)
    else:
        sys.exit("No valid semantic version tags found.")


if __name__ == "__main__":
    repository = "astronomer/astro-runtime"
    if len(sys.argv) == 2 and sys.argv[1]:
        repository = sys.argv[1]
    latest_tag = get_latest_tag(repository=repository)
    print(latest_tag)
