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
REPO_DIR = Path(__file__).parent.parent

# -- Project information -----------------------------------------------------

PROJECT = "Astronomer Providers"
AUTHOR = "Astronomer Inc."

# The full version, including alpha/beta/rc tags
RELEASE = "1.3.0.dev1"


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
EXTENSIONS = [
    "autoapi.extension",
    "sphinx.ext.intersphinx",
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
]

# Add any paths that contain templates here, relative to this directory.
TEMPLATES_PATH = ["_templates"]

# If true, keep warnings as "system message" paragraphs in the built documents.
KEEP_WARNINGS = True

# The master toctree document.
MASTER_DOC = "index"

# -- Options for HTML output ---------------------------------------------------
# See: https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output


# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
EXCLUDE_PATTERNS = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
HTML_THEME = "alabaster"

# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
#
HTML_THEME_OPTIONS = {
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
AUTOAPI_DIRS = ["../astronomer"]

AUTOAPI_TEMPLATE_DIR = "_templates/autoapi"
AUTOAPI_GENERATE_API_DOCS = True

# The default options for autodoc directives. They are applied to all autodoc directives automatically.
AUTODOC_DEFAULT_OPTIONS = {"show-inheritance": True, "members": True}

AUTODOC_TYPEHINTS = "description"
AUTODOC_TYPEHINTS_DESCRIPTION_TARGET = "documented"
AUTODOC_TYPEHINTS_FORMAT = "short"

# Keep the AutoAPI generated files on the filesystem after the run.
# Useful for debugging.
AUTOAPI_KEEP_FILES = True

# Relative path to output the AutoAPI files into. This can also be used to place the generated documentation
# anywhere in your documentation hierarchy.
AUTOAPI_ROOT = "_api"

# Whether to insert the generated documentation into the TOC tree. If this is False, the default AutoAPI
# index page is not generated and you will need to include the generated documentation in a
# TOC tree entry yourself.
AUTOAPI_ADD_TOCTREE_ENTRY = True

# By default autoapi will include private members -- we don't want that!
AUTOAPI_OPTIONS = [
    "members",
    "undoc-members",
    "show-inheritance",
    "show-module-summary",
    "special-members",
]

# Ignore example DAGs from the API docs
AUTOAPI_IGNORE = [
    "*example_dags*",
]

SUPPRESS_WARNINGS = [
    "autoapi.python_import_resolution",
    "ref.doc",
]

AUTOAPI_PYTHON_USE_IMPLICIT_NAMESPACES = True

# -- Intersphinx configuration ------------------------------------------------
# Get all the providers from setup.cfg and use them to generate the intersphinx inventories
# for all the providers
CONFIG = configparser.ConfigParser(strict=False)
CONFIG.read(REPO_DIR / "setup.cfg")

PROV_DEPS = [
    re.match(r"([a-zA-Z-]+)", dep).groups()[0]
    for dep in CONFIG["options.extras_require"]["all"].split()
    if dep.startswith("apache-airflow-providers-")
]
INTERSPHINX_MAPPING = {
    "airflow": ("https://airflow.apache.org/docs/apache-airflow/stable/", None),
    **{provider: (f"https://airflow.apache.org/docs/{provider}/stable", None) for provider in PROV_DEPS},
}


# This explicitly defines __init__ not to be skipped (which it is by default).
def _inclue_init_in_docs(app, what, name, obj, would_skip, options):
    """This is needed to document __init__ from parent classes."""
    if name == "__init__":
        return False
    return would_skip


def setup(app):
    """Sphinx Application API"""
    if "autoapi.extension" in EXTENSIONS:
        app.connect("autodoc-skip-member", _inclue_init_in_docs)
