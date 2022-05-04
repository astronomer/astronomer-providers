#!/usr/bin/env python3
"""Verify the version of the Package with the version in Git tag."""
import configparser
import os
from pathlib import Path

repo_dir = Path(__file__).parent.parent.parent

config = configparser.ConfigParser(strict=False)
config.read(repo_dir / "setup.cfg")

version_in_setup_cfg = config["metadata"]["version"]
git_tag = os.getenv("CIRCLE_TAG")

if git_tag is not None:
    if version_in_setup_cfg != git_tag:
        raise SystemExit(
            f"The version in setup.cfg ({version_in_setup_cfg}) does not match the Git Tag ({git_tag})."
        )
