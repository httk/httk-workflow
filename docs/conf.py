import importlib
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
    "httk.core.docs.sphinx_ext",
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
autoapi_ignore = []  # scan everything; the reference is curated by skip_member below

autoapi_type = "python"
autoapi_dirs = ["../src/httk"]
autoapi_add_toctree_entry = False
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

# A documented public signature routinely annotates a type whose *definition*
# lives in a module that is deliberately not part of the reference (the protocol
# implementation, the manager, the runtime builders, the CLI). The type is
# documented at its public home, but AutoAPI resolves the annotation against the
# defining module, so those cross-references cannot land and are ignored here
# rather than pulling the internal modules back into the reference. The private
# type aliases the same signatures mention are ignored for the same reason.
_INTERNAL_MODULES = (
    "models",
    "journal",
    "transactions",
    "runtime_builders",
    "workspace",
    "manager",
    "introspection",
    "gc",
    "fsck",
    "adapter_runtime",
    "cli",
    "workflow_cli",
    # Compatibility internals: the engines are public, their runners and the
    # v1 CLI alias and shared import tail are not.
    "compat._integration",
    "compat.cwl.cwl_runner",
    "compat.pwd.pwd_runner",
    "compat.v1._runner",
    "compat.v1.cli",
    # The VASP facade is public; the cohesive modules it re-exports are not.
    "vasp.inputs",
    "vasp.diagnostics",
    "vasp.remedies",
    "vasp.reports",
    "vasp.workflows",
)
nitpick_ignore_regex = [
    (r"py:.*", r"httk\.workflow\.(" + "|".join(_INTERNAL_MODULES) + r")(\..+)?"),
    (r"py:.*", r"httk\.workflow\.vasp\.runners(\..+)?"),
    (
        r"py:.*",
        r"(DataMode|WorkdirMode|PublishMode|RunnerSource|StepHandler|JoinCondition"
        r"|DiagnosticSeverity|EventMonitor|RemedyChange|RemedySequence|MarkerFault|V1Materializer)",
    ),
]
copybutton_prompt_text = r">>> |\.\.\. |\$ "
copybutton_prompt_is_regexp = True

suppress_warnings = ["myst.xref_missing", "autoapi.python_import_resolution"]

# The API reference documents a deliberate public surface, not every source
# object. Two rules, applied by ``skip_member`` below, decide what appears:
#
# 1. Only the modules named here get a reference page. Everything else — manager
#    internals, the CLI, the subprocess bridges, and the implementation modules
#    behind :mod:`httk.workflow.protocol` — is still scanned (so a public page
#    can document a name it re-exports) but produces no page of its own. This is
#    the "three levels" of the package: the filesystem protocol, the execution
#    and authoring surface, and orchestration plus management.
# 2. Within a documented module, only the names it lists in ``__all__`` appear.
#    A module without ``__all__`` (the two namespace packages) is documented by
#    the underscore rule alone. This is what drops the helpers a package merely
#    imports (for example the supervision types :mod:`httk.workflow.vasp` uses)
#    without any per-name suppression list.
PUBLIC_MODULES = frozenset(
    {
        "httk.workflow",
        # Filesystem protocol.
        "httk.workflow.protocol",
        "httk.workflow.errors",
        # Execution / authoring.
        "httk.workflow.sdk",
        "httk.workflow.runtime",
        "httk.workflow.runtime_utils",
        "httk.workflow.scaffold",
        "httk.workflow.executors",
        "httk.workflow.shell_bridge",
        # Orchestration and management.
        "httk.workflow.collecting",
        "httk.workflow.provenance",
        "httk.workflow.supervision",
        "httk.workflow.transfers",
        "httk.workflow.manifests",
        "httk.workflow.hygiene",
        "httk.workflow.adapters",
        "httk.workflow.adapter_protocol",
        "httk.workflow.configuration",
        "httk.workflow.projects",
        # Domain and compatibility consumers.
        "httk.workflow.vasp",
        "httk.workflow.compat",
        "httk.workflow.compat.v1",
        "httk.workflow.compat.cwl",
        "httk.workflow.compat.pwd",
    }
)

_exports_cache: dict[str, frozenset[str] | None] = {}


def _module_exports(module_name):
    """Return the ``__all__`` of one module, or ``None`` when it declares none."""

    if module_name not in _exports_cache:
        try:
            module = importlib.import_module(module_name)
        except Exception:  # pragma: no cover - a module that will not import
            _exports_cache[module_name] = None
        else:
            names = getattr(module, "__all__", None)
            _exports_cache[module_name] = None if names is None else frozenset(names)
    return _exports_cache[module_name]


def skip_member(app, what, name, obj, skip, options):
    obj_id = str(getattr(obj, "id", None) or name)
    if what in {"module", "package"}:
        # Only the deliberate public modules get a page; the rest stay scanned
        # (so re-exports resolve) but unrendered.
        return obj_id not in PUBLIC_MODULES
    if name.startswith("_"):
        return True
    owner, _, short = obj_id.rpartition(".")
    exports = _module_exports(owner)
    if exports is not None and short not in exports:
        return True
    return skip


def setup(sphinx):
    sphinx.connect('autoapi-skip-member', skip_member)
