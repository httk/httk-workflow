import os
import warnings
from datetime import date

from sphinx.deprecation import RemovedInSphinx10Warning
warnings.filterwarnings("ignore", category=RemovedInSphinx10Warning)

project = "httk-workflow"
author = "The httk-workflow AUTHORS"
copyright = f"{date.today().year}, {author}"

extensions = [
    # Core API docs
    "sphinx.ext.autodoc",        # pull docstrings
    "sphinx.ext.autosummary",    # API summary tables + stub gen
    "sphinx.ext.napoleon",       # Google/NumPy docstrings
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "sphinx.ext.mathjax",        # math rendering via MathJax

    # Nice-to-haves
    "sphinx_autodoc_typehints",
    "sphinx_copybutton",

    # Markdown + notebooks
    "myst_nb",                   # .ipynb support

    "autoapi.extension",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "**/.ipynb_checkpoints"]

# Autosummary: generate stub pages automatically
autosummary_generate = True

# Autodoc defaults (tweak to taste)
autodoc_default_options = {
    "members": True,
    "member-order": "bysource",
    "undoc-members": False,
    "show-inheritance": True,
}
autodoc_typehints = "signature"
typehints_fully_qualified = False
typehints_document_rtype = True
typehints_defaults = "comma"
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_attr_annotations = True

# MyST / Markdown configuration (math + nice syntax)
myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "fieldlist",
    "substitution",
    "tasklist",
    "dollarmath",  # enables $...$ and $$...$$
]
myst_heading_anchors = 3

# myst-nb config: don't execute notebooks during docs build by default
nb_execution_mode = "off"

html_theme = "furo"
html_theme_options = {
    "sidebar_hide_name": False,
    "navigation_with_keys": True,
}

# External references resolve against inventories vendored in docs/_inventories/
# so docs builds need no network access; link targets still point at the live
# sites. Refresh the committed inventories with `make docs-inventories`.
#
# When this module cross-references public objects from another httk
# distribution (e.g. httk.core), add it here against the published httk docs
# site. The base URL comes from the DOCS_BASE_URL Makefile variable (exported as
# HTTK_DOCS_BASE_URL); the default below keeps bare sphinx invocations working.
# Vendor each dependency inventory alongside python.inv, for example:
#     "httk-core": (f"{_docs_base_url}/httk-core/", "_inventories/httk-core.inv"),
_docs_base_url = os.environ.get("HTTK_DOCS_BASE_URL", "https://docs.httk.org")

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", "_inventories/python.inv"),
    "httk-core": (f"{_docs_base_url}/httk-core/", "_inventories/httk-core.inv"),
}

autoapi_options = [
       "members",
       "undoc-members",
       "show-inheritance",
       "show-module-summary",
       "imported-members",
]
autoapi_root = "reference/autoapi"
autoapi_ignore = []  # include everything

autoapi_type = "python"
autoapi_dirs = ["../src/httk"]
autoapi_add_toctree_entry = True
autoapi_keep_files = True
autoapi_member_order = "bysource"
autoapi_python_class_content = "module"  # docstring under class, not merged from __init__
autoapi_python_use_implicit_namespaces = True
autoapi_template_dir = "_templates/autoapi"

nitpicky = True
nitpick_ignore = [
    ("py:class", "typing.Any"),
    ("py:class", "typing.Optional"),
    ("py:class", "typing.Union"),
    ("py:class", "Ellipsis"),
    # The command tree annotates the action `add_subparsers` returns, which is
    # a private stdlib class the Python documentation has no page for.
    ("py:class", "argparse._SubParsersAction"),
]
copybutton_prompt_text = r">>> |\.\.\. |\$ "
copybutton_prompt_is_regexp = True

suppress_warnings = ["myst.xref_missing", "autoapi.python_import_resolution"]

# Names that a submodule and a re-export of the package share. The Python domain
# holds one object per fully qualified name, so documenting both
# ``httk.workflow.harvest`` the module and ``httk.workflow.harvest`` the function
# the package re-exports is a duplicate. The module page documents the function
# in full, so the re-export is what gets dropped from the package page.
shadowed_by_module = {"httk.workflow.harvest"}

# Members that reach a package page only because its ``__init__`` imports them.
# ``imported-members`` is what documents a package's deliberate re-exports, and
# for ``httk.workflow`` that is exactly the point. A subpackage whose ``__init__``
# is one module of implementation is the other case: documenting the helpers it
# merely uses would repeat pages that already carry them, on a page whose
# namespace does not contain the names those signatures reference.
borrowed_by_package = {
    "httk.workflow.vasp": frozenset(
        {
            "JOB_STATE_DIRECTORY",
            "Diagnostic",
            "FollowSource",
            "ProcessReport",
            "ProcessSupervisor",
            "ReplayableWorkdirBatch",
            "SourceEvent",
            "read_json",
            "sha256_file",
            "utc_now",
            "write_json_atomic",
        }
    ),
}


def skip_member(app, what, name, obj, skip, options):
    # Skip private members (those starting with _)
    if name.startswith('_'):
        return True
    if what != "module" and name in shadowed_by_module:
        return True
    owner, _, short = str(getattr(obj, "id", None) or name).rpartition(".")
    if short in borrowed_by_package.get(owner, frozenset()):
        return True
    return skip

def setup(sphinx):
    sphinx.connect('autoapi-skip-member', skip_member)
