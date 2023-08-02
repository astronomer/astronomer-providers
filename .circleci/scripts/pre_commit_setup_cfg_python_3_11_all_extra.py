#!/usr/bin/env python3
"""
Pre-commit hook to sync a "test_python_3_11" extra in setup.cfg.
It will contain all the dependencies apart from tests and mypy.
"""
import configparser
from pathlib import Path

repo_dir = Path(__file__).parent.parent.parent

config = configparser.ConfigParser(strict=False)
config.read(repo_dir / "setup.cfg")

extra_to_exclude = {"tests", "mypy", "docs", "all", "test_python_3_11", "apache.hive"}
expected_test_python_3_11_extra = {
    req
    for key, extra_value in config["options.extras_require"].items()
    for req in extra_value.split()
    if key not in extra_to_exclude
}

found_test_python_3_11_extra = set(config["options.extras_require"].get("test_python_3_11", "").split())
if not found_test_python_3_11_extra:
    raise SystemExit("Missing 'test_python_3_11' extra in setup.cfg")

"""
Use XOR operator ^ to find the missing dependencies instead of set A - set B
set A - set B will only show difference of set A from set B, but we want see overall diff
"""
diff_extras = expected_test_python_3_11_extra ^ found_test_python_3_11_extra
if diff_extras:
    diff_extras_str = "\n \t" + "\n \t".join(sorted(diff_extras))
    raise SystemExit(
        f"'test_python_3_11' extra in setup.cfg is missing some dependencies:\n {diff_extras_str}"
    )
