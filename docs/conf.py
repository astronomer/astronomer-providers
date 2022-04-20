# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

import configparser

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, os.path.abspath(".."))
_REPO_DIR = Path(__file__).parent.parent

# -- Project information -----------------------------------------------------

project = "Astronomer Providers"
author = "Astronomer Inc."

# The full version, including alpha/beta/rc tags
release = "1.3.0.dev1"


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "autoapi.extension",
    "sphinx.ext.intersphinx",
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# If true, keep warnings as "system message" paragraphs in the built documents.
keep_warnings = True

# The master toctree document.
master_doc = "index"

# -- Options for HTML output ---------------------------------------------------
# See: https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output


# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "alabaster"

# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
#
html_theme_options = {
    "description": "Airflow Providers containing Deferrable Operators & Sensors from Astronomer",
    "github_user": "astronomer",
    "github_repo": "astronomer-providers",
}


# Custom sidebar templates, maps document names to template names.
#
# html_sidebars = {}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
# html_static_path = ["_static"]

# -- AutoAPI ---------------------------------------------------------------
autoapi_dirs = ["../astronomer"]

autoapi_template_dir = "_templates/autoapi"
autoapi_generate_api_docs = True

# The default options for autodoc directives. They are applied to all autodoc directives automatically.
autodoc_default_options = {"show-inheritance": True, "members": True}

autodoc_typehints = "description"
autodoc_typehints_description_target = "documented"
autodoc_typehints_format = "short"

# Keep the AutoAPI generated files on the filesystem after the run.
# Useful for debugging.
autoapi_keep_files = True

# Relative path to output the AutoAPI files into. This can also be used to place the generated documentation
# anywhere in your documentation hierarchy.
autoapi_root = "_api"

# Whether to insert the generated documentation into the TOC tree. If this is False, the default AutoAPI
# index page is not generated and you will need to include the generated documentation in a
# TOC tree entry yourself.
autoapi_add_toctree_entry = True

# By default autoapi will include private members -- we don't want that!
autoapi_options = [
    "members",
    "undoc-members",
    "show-inheritance",
    "show-module-summary",
    "special-members",
]

# Ignore example DAGs from the API docs
autoapi_ignore = [
    "*example_dags*",
]

suppress_warnings = [
    "autoapi.python_import_resolution",
    "ref.doc",
]

autoapi_python_use_implicit_namespaces = True

# -- Intersphinx configuration ------------------------------------------------
# Get all the providers from setup.cfg and use them to generate the intersphinx inventories
# for all the providers
config = configparser.ConfigParser(strict=False)
config.read(_REPO_DIR / "setup.cfg")

_PROV_DEPS = [
    re.match(r"([a-zA-Z-]+)", dep).groups()[0]
    for dep in config["options.extras_require"]["all"].split()
    if dep.startswith("apache-airflow-providers-")
]
intersphinx_mapping = {
    "airflow": ("https://airflow.apache.org/docs/apache-airflow/stable/", None),
    **{provider: (f"https://airflow.apache.org/docs/{provider}/stable", None) for provider in _PROV_DEPS},
}


# This explicitly defines __init__ not to be skipped (which it is by default).
def _inclue_init_in_docs(app, what, name, obj, would_skip, options):
    """This is needed to document __init__ from parent classes."""
    if name == "__init__":
        return False
    return would_skip


def setup(app):
    """Sphinx Application API"""
    if "autoapi.extension" in extensions:
        app.connect("autodoc-skip-member", _inclue_init_in_docs)
