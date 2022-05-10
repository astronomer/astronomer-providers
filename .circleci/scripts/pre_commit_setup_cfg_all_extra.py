#!/usr/bin/env python3
"""
Pre-commit hook to sync an "all" extra in setup.cfg.
It will contain all the dependencies apart from tests and mypy.
"""
import configparser
import os
from pathlib import Path

import docutils
from docutils.core import publish_doctree

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


def extract_block(block_node, _type, cond):
    """Extract block base on block type and given condition"""
    for n in block_node.children:
        if isinstance(n, _type) and n["ids"] == cond:
            return n
        extract_block(n, _type, cond)


def extract_table_block(block_node, _type):
    """Extract table body from extra node section"""
    for child_node in block_node.children:
        if isinstance(child_node, _type):
            return child_node
        result = extract_table_block(child_node, _type)
        if result:
            return result


with open(os.path.join(os.path.dirname(__file__), repo_dir / "README.rst")) as f:
    document = publish_doctree(f.read())
    nodes = list(document)
    extra_node = None
    for node in nodes:
        extra_node = extract_block(node, docutils.nodes.section, ["extras"])
        if extra_node is not None:
            break

    table_body = extract_table_block(extra_node, docutils.nodes.tbody)
    extra_list = []
    for row in table_body:
        for val in row[0].children:
            if val.children[0].astext() != "all":
                extra_list.append(val.children[0].astext())
    extra = all_extra
    extra.sort()
    if extra != all_extra:
        SystemExit("Extras in not in sorted order in setup.cfg")

    if extra != extra_list:
        SystemExit("Extras is missing or not in sorted order in README.rst")
