#!/usr/bin/env python3
"""
Pre-commit hook to sync an "all" extra in setup.cfg.
It will contain all the dependencies apart from tests and mypy.
"""
import configparser
from pathlib import Path

repo_dir = Path(__file__).parent.parent.parent

config = configparser.ConfigParser(strict=False)
config.read(repo_dir / "setup.cfg")

all_extra = []
extra_to_exclude = {"tests", "mypy", "docs", "all"}
for k in config["options.extras_require"].keys():
    if k in extra_to_exclude:
        continue
    reqs = config["options.extras_require"][k].split()
    all_extra.extend(reqs)

expected_all_extra = set(all_extra)
found_all_extra = set(config["options.extras_require"].get("all", "").split())
if not found_all_extra:
    raise SystemExit("Missing 'all' extra in setup.cfg")

"""
Use XOR operator ^ to find the missing dependencies instead of set A - set B
set A - set B will only show difference of set A from set B, but we want see overall diff
"""
diff_extras = expected_all_extra ^ found_all_extra
if diff_extras:
    diff_extras_str = "\n \t" + "\n \t".join(sorted(diff_extras))
    raise SystemExit(f"'all' extra in setup.cfg is missing some dependencies:\n {diff_extras_str}")
