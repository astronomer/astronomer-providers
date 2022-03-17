#!/usr/bin/env python3
"""Pre-commit hook to sync the "version" in setup.cfg and version in docs/conf.py."""
import configparser
import re
from pathlib import Path

repo_dir = Path(__file__).parent.parent.parent

config = configparser.ConfigParser(strict=False)
config.read(repo_dir / "setup.cfg")

version_in_setup_cfg = config["metadata"]["version"]

conf_py_path = repo_dir / "docs" / "conf.py"

with open(conf_py_path, "r") as f:
    conf_py_contents = f.read()

new_text = re.sub(
    r"release =(.*)", f'release = "{version_in_setup_cfg}"', conf_py_contents, flags=re.MULTILINE
)

with open(str(repo_dir / "docs/conf.py"), "w") as f:
    f.write(new_text)
