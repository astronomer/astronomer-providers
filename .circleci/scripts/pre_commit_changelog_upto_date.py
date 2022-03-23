#!/usr/bin/env python3
"""Pre-commit hook to verify changelog is updated when releasing a new version."""
import configparser
from pathlib import Path

from packaging.version import Version

repo_dir = Path(__file__).parent.parent.parent

config = configparser.ConfigParser(strict=False)
config.read(repo_dir / "setup.cfg")

version_in_setup_cfg = config["metadata"]["version"]
version = Version(version_in_setup_cfg)

changelog_path = repo_dir / "CHANGELOG.rst"

with open(changelog_path, "r") as f:
    changelog_contents = f.read()

if not version.is_devrelease and version_in_setup_cfg not in changelog_contents:
    raise SystemExit(f"Version in setup.cfg ({version_in_setup_cfg}) is not in changelog ({changelog_path}).")
