#!/usr/bin/env python3
"""Pre-commit hook to verify that all extras are documented in README.rst"""
import configparser
import re
from pathlib import Path

repo_dir = Path(__file__).parent.parent.parent

config = configparser.ConfigParser(strict=False)
config.read(repo_dir / "setup.cfg")

all_extra = []
extra_to_exclude = {"tests", "mypy", "docs"}
all_extras = set(config["options.extras_require"].keys()) - extra_to_exclude

readme_path = repo_dir / "README.rst"
extra_doc = """

.. list-table::
   :header-rows: 1

   * - Extra Name
     - Installation Command
     - Dependencies
"""


for extra in sorted(all_extras):

    extra_doc += f"""
   * - ``{extra}``
     - ``pip install 'astronomer-providers[{extra}]'``
     - {extra.replace(".", " ").title()}
"""


with open(readme_path, "r") as readme_file:
    readme_contents = readme_file.read()
    new_readme_text = re.sub(
        r".. EXTRA_DOC_START([\s\S]*).. EXTRA_DOC_END",
        f".. EXTRA_DOC_START{extra_doc}\n.. EXTRA_DOC_END",
        readme_contents,
        flags=re.MULTILINE,
    )

if new_readme_text != readme_contents:
    with open(readme_path, "w") as readme_file:
        readme_file.write(new_readme_text)
