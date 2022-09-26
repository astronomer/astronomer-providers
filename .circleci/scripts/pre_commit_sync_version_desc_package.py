#!/usr/bin/env python3
"""Pre-commit hook to sync the "version" & "description" in setup.cfg and astronomer/providers/package.py."""
import configparser
import re
from pathlib import Path

repo_dir = Path(__file__).parent.parent.parent

config = configparser.ConfigParser(strict=False)
config.read(repo_dir / "setup.cfg")

version_in_setup_cfg = config["metadata"]["version"]
description_in_setup_cfg = config["metadata"]["description"]

package_py = repo_dir / "astronomer/providers/package.py"

with open(package_py, "r") as f:
    package_py_contents = f.read()

new_text = re.sub(
    r'versions": (.*)', f'versions": "{version_in_setup_cfg}",', package_py_contents, flags=re.MULTILINE
)

new_text = re.sub(
    r'description": (.*)', f'description": "{description_in_setup_cfg}",', new_text, flags=re.MULTILINE
)

with open(package_py, "w") as f:
    f.write(new_text)
