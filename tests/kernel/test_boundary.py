"""The race boundary (plan §1 P1): which modules may touch the filesystem in ways other actors can observe.

Every rename, removal and exclusive creation that another process can race
with must go through :mod:`httk.workflow._fs`, and every contested move through
:mod:`httk.workflow._kernel`, so that the two reviewed surfaces really own every
race. The scan reads the syntax tree, so comments and docstrings never count.
"""

import ast
from collections.abc import Mapping
from functools import cache
from pathlib import Path

SOURCE = Path(__file__).resolve().parents[2] / "src" / "httk" / "workflow"

#: Raw operations by module; ``os.remove``, ``os.renames`` and ``os.removedirs`` unlink, rename and rmdir too.
RAW_CALLS: Mapping[str, frozenset[str]] = {
    "os": frozenset({"rename", "renames", "replace", "link", "unlink", "remove", "removedirs", "rmdir", "symlink"}),
    "shutil": frozenset({"rmtree", "move"}),
    "fcntl": frozenset({"flock", "lockf"}),
}
#: Creating a file with ``O_CREAT`` is a raw operation wherever the flag is named.
RAW_FLAG = frozenset({"O_CREAT"})
#: Pathlib-style calls on any object are raw too: ``.unlink(``, ``.rename(``, ``.rmdir(``, and ``.replace(`` with
#: exactly one positional argument and no keywords (``str.replace`` takes two, ``Path.replace`` one).
RAW_METHODS = frozenset({"unlink", "rename", "rmdir", "replace"})
CONTESTED = frozenset({"move_once", "publish_dir", "publish_record"})
#: Never in any module: no hard links, no symlinks created, no file locks.
LINKS = {"os": frozenset({"link", "symlink"})}
#: The one exception: a trusted ``_fs.copy_tree`` recreates a symlink it copies as a symlink (an untrusted copy,
#: one with limits, refuses symlinks).
LINKS_ALLOWED = {"_fs.py": {"os.symlink"}}
#: Temporary files and copies by name: each creates entries no ``_fs`` primitive vouches for.
TEMPFILE = {
    "tempfile": frozenset(
        {
            "mkstemp",
            "mkdtemp",
            "mktemp",
            "TemporaryFile",
            "NamedTemporaryFile",
            "SpooledTemporaryFile",
            "TemporaryDirectory",
        }
    )
}
SHUTIL_COPY = {"shutil": frozenset({"copy", "copy2", "copyfile", "copytree", "copymode", "copystat"})}
#: The JSON helpers of ``_util``: a plain-path read and a ``mkstemp`` write, neither anchored nor bounded.
UTIL_JSON = frozenset({"read_json", "write_json_atomic"})
#: Directory creation outside ``_fs``: ``os.mkdir``/``os.makedirs`` and pathlib's ``.mkdir(``.
MKDIR = frozenset({"mkdir", "makedirs"})

#: The modules that implement the workspace protocol: they write protocol entries only through ``_fs``.
PROTOCOL = frozenset(
    {
        "_bundles.py",
        "_data.py",
        "_death.py",
        "_exchange.py",
        "_fs.py",
        "_kernel.py",
        "_moving.py",
        "_store.py",
        "fsck.py",
        "gc.py",
        "manager.py",
        "removal.py",
        "seals.py",
        "workspace.py",
    }
)
LOCKS = frozenset({"flock", "lockf"})

#: The runner side acts only inside its own attempt directory (P1).
RUNNER_SIDE = frozenset(
    {
        # Inside the attempt sandbox: publishes its own launch requests below the attempt's launch/ directory.
        "_launch_client.py",
        "runtime_builders.py",
        "sdk.py",
    }
)
#: Modules whose only raw operations are pathlib-style calls on entries private to one actor.
METHOD_RAW = frozenset(
    {
        # Adapter side: removes its own temporary rsync file listing.
        "adapter_runtime.py",
        # v1 realization: renders templates inside the payload it is staging (private scratch or attempt).
        "compat/v1/realization.py",
        "compat/v1/templates.py",
    }
)
# legacy: shrinks per phase; never add to it.
LEGACY_RAW = frozenset(
    {
        "_logging.py",
        "_util.py",
        "adapters.py",
        "compat/cwl/cwl_runner.py",
        "compat/jobflow/jobflow_runner.py",
        "compat/v1/v1_runner.py",
        "hygiene.py",
        "launchers.py",
        "registry.py",
        "runtime_utils.py",
        "workflow_cli/_workspace.py",
    }
)
#: Who may call each contested primitive besides its definition in ``_fs.py``.
CONTESTED_CALLERS: Mapping[str, frozenset[str]] = {
    # The daemon ledger fences a stalled instance's prepared anchor; a client's take-back races the managers'
    # take of the same inbox entry.
    "move_once": frozenset({"_kernel.py", "_daemon_state.py", "_daemon_client.py"}),
    # The daemon ledger's prepared anchors and the ledger itself.
    "publish_dir": frozenset({"_kernel.py", "_daemon_state.py"}),
    # The daemon ledger's records and the client's signed request cache: one record per name, at most once.
    "publish_record": frozenset({"_daemon_state.py", "_daemon_client.py"}),
}
#: Who may create temporaries through ``tempfile``; none of them names an entry of the workspace protocol.
TEMPFILE_CALLERS = frozenset(
    {
        # Remote and launcher bundle requests: a private request file in the system temporary directory, and the
        # adapter metadata rewrite inside a bundle being imported.
        "adapters.py",
        "launchers.py",
        # Adapter side: its own rsync file listing in the system temporary directory.
        "adapter_runtime.py",
        # Runner side: templates rendered inside the attempt or payload the runner owns.
        "runtime_utils.py",
        "compat/v1/v1_runner.py",
        # Import-only; the CWL realization stages documents in a private temporary directory.
        "compat/cwl/__init__.py",
        # An anonymous file (no name anywhere) buffering a seal's manifest.
        "seals.py",
        # CLI: a relay directory for a remote-to-remote transfer and the payload a job command is built in, both in
        # the system temporary directory.
        "workflow_cli/_transfer.py",
        "workflow_cli/_job.py",
        # write_json_atomic itself, whose own callers are listed in UTIL_JSON_CALLERS.
        "_util.py",
    }
)
#: Who may copy by name with ``shutil``; every destination is private to one actor until it is moved through ``_fs``.
SHUTIL_COPY_CALLERS = frozenset(
    {
        # Remote and launcher bundles copied from the packaged templates into a project's configuration.
        "adapters.py",
        "launchers.py",
        # Adapter side: a local or mounted push/pull into a fresh name (incoming/<T>.<t>, a landing scratch).
        "adapter_runtime.py",
        # Runner side: copies into the runner's own attempt, workdir or transaction staging.
        "runtime_builders.py",
        "sdk.py",
        "_data.py",
        "compat/cwl/cwl_runner.py",
        "compat/v1/v1_runner.py",
        "compat/v1/templates.py",
        # Preparing a payload in a private scratch before it is submitted through the kernel.
        "scaffold.py",
        "compat/cwl/__init__.py",
        "compat/jobflow/__init__.py",
        # Store installations, built in the owner's build scratch and installed by rename.
        "_store.py",
    }
)
#: Who may use ``_util.read_json``/``write_json_atomic``: no protocol module (they read and write control and job
#: files through ``_fs``).
UTIL_JSON_CALLERS = frozenset(
    {
        # Project configuration and adapter/launcher bundle metadata, outside every workspace.
        "configuration.py",
        "registry.py",
        "adapters.py",
        "launchers.py",
        "workflow_cli/_daemon_remote.py",
        "workflow_cli/_transfer.py",
        # Hygiene reads the metadata of holds and copies for its report only.
        "hygiene.py",
        # Runner side: documents inside the runner's own attempt and payload, and the files a user names.
        "runtime_builders.py",
        "sdk.py",
        "_shell_bridge.py",
        "compat/v1/v1_runner.py",
        # Re-exported for code support packages, which work inside their own attempt.
        "codes/__init__.py",
    }
)
#: The directory creations in protocol modules that do not go through ``_fs``, by function.
MKDIR_ALLOWED = frozenset(
    {
        # Anchored at the payload's attempts/ descriptor (opened through _fs.open_dir_under): the exclusive mkdir
        # of a fresh attempt id is the attempt's creation.
        "manager.py:TaskManager._start_attempt",
        # The daemon ledger's dot-named staging, published at most once with _fs.publish_dir, and its prepared
        # anchors, each an exclusive mkdir of a fresh name in the daemon's private state.
        "_daemon_state.py:Ledger._initialize",
        "_daemon_state.py:Ledger._prepare",
        # Daemon setup creates its private state directories component by component, O_NOFOLLOW, by descriptor.
        "_daemon_setup.py:_mkdir_exclusive",
        # Workspace creation: no other actor shares the workspace before format.json exists, and the exclusive
        # mkdir of .httk-workspace is the creation's claim.
        "workspace.py:Workspace.initialize",
        # Runner side: a transaction staged in the runner's own attempt directory.
        "_data.py:Transaction.__init__",
        "_data.py:Transaction.put",
        # Store installations, built in the owner's private build scratch.
        "_store.py:_adhoc_package",
        "_store.py:_build_into",
        "_store.py:_document_package",
        "_store.py:_install",
    }
)


def _modules() -> list[str]:
    return sorted(path.relative_to(SOURCE).as_posix() for path in SOURCE.rglob("*.py"))


@cache
def _tree(module: str) -> ast.Module:
    return ast.parse((SOURCE / module).read_text(encoding="utf-8"), filename=module)


def _module_uses(tree: ast.Module, wanted: Mapping[str, frozenset[str]]) -> set[str]:
    """Every ``<module>.<name>`` of *wanted* that *tree* references, through any import form."""

    aliases: dict[str, str] = {}
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                # "import os.path" binds "os"; "import os as o" binds "o".
                imported = alias.name if alias.asname else alias.name.split(".")[0]
                if imported in wanted:
                    aliases[alias.asname or imported] = imported
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and (module := node.module or "") in wanted:
            found.update(f"{module}.{alias.name}" for alias in node.names if alias.name in wanted[module])
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
            target = aliases.get(node.value.id)
            if target is not None and node.attr in wanted[target]:
                found.add(f"{target}.{node.attr}")
    return found


def _names_used(tree: ast.Module, names: frozenset[str]) -> set[str]:
    """Every name of *names* referenced as an attribute, a bare name or an imported name."""

    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and node.attr in names:
            found.add(node.attr)
        elif isinstance(node, ast.Name) and node.id in names:
            found.add(node.id)
        elif isinstance(node, ast.ImportFrom):
            found.update(alias.name for alias in node.names if alias.name in names)
    return found


def _method_calls(tree: ast.Module, names: frozenset[str]) -> set[str]:
    """Every ``.<name>(`` call of *names* on any object; ``.replace(`` only with one argument and no keywords."""

    found: set[str] = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in names
            and (node.func.attr != "replace" or (len(node.args) == 1 and not node.keywords))
        ):
            found.add(f".{node.func.attr}")
    return found


def _raw_uses(tree: ast.Module) -> set[str]:
    return _module_uses(tree, RAW_CALLS) | _names_used(tree, RAW_FLAG) | _method_calls(tree, RAW_METHODS)


def _protocol(module: str) -> bool:
    return module in PROTOCOL or module.startswith("_daemon_")


def _util_json(tree: ast.Module) -> set[str]:
    """Every name of :data:`UTIL_JSON` imported from ``_util`` (any spelling) or read as ``_util.<name>``."""

    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and (node.module or "").split(".")[-1] == "_util":
            found.update(alias.name for alias in node.names if alias.name in UTIL_JSON)
        elif isinstance(node, ast.Attribute) and node.attr in UTIL_JSON and getattr(node.value, "id", None) == "_util":
            found.add(node.attr)
    return found


def _calls_by_function(tree: ast.Module, names: frozenset[str]) -> set[str]:
    """The qualified name of every function (``<module>`` at top level) calling a name of *names*, as
    ``f(``, ``x.f(`` or ``os.f(``."""

    found: set[str] = set()

    def visit(node: ast.AST, scope: tuple[str, ...]) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
                visit(child, (*scope, child.name))
                continue
            if isinstance(child, ast.Call):
                function = child.func
                called = function.attr if isinstance(function, ast.Attribute) else getattr(function, "id", None)
                if called in names:
                    found.add(".".join(scope) or "<module>")
            visit(child, scope)

    visit(tree, ())
    return found


def test_scanner_sees_every_spelling_and_ignores_prose() -> None:
    # The rules are only as good as the scanner: check it against each import form and against prose.
    tree = ast.parse(
        '''
"""os.rename(a, b) and move_once( in a docstring do not count."""
import os.path
import shutil as sh
from fcntl import flock
from httk.workflow._fs import publish_dir

# os.unlink(x) in a comment does not count


def f(o, _fs):
    os.replace(a, b)
    sh.rmtree(d)
    o.rename(x)  # another object's method
    path.unlink(missing_ok=True)
    text.replace("a", "b")  # str.replace: two arguments, not counted
    _fs.move_once(s, d)
    return os.O_WRONLY | os.O_CREAT
'''
    )
    assert _raw_uses(tree) == {
        "os.replace",
        "shutil.rmtree",
        "fcntl.flock",
        "O_CREAT",
        ".rename",
        ".unlink",
    }
    assert _method_calls(ast.parse("p.replace(q)\ns.replace(a, b)\np.rmdir()\n"), RAW_METHODS) == {
        ".replace",
        ".rmdir",
    }
    assert _names_used(tree, CONTESTED) == {"move_once", "publish_dir"}
    assert _module_uses(ast.parse("import os\nos.link(a, b)\nos.symlink(a, b)\n"), LINKS) == {"os.link", "os.symlink"}
    spelled = ast.parse(
        """
from tempfile import mkstemp
import shutil
from .._util import read_json
from httk.workflow import _util


class A:
    def f(self, p):
        p.mkdir()
        shutil.copytree(a, b)
        _util.write_json_atomic(p, {})


def g():
    os.makedirs(x)
"""
    )
    assert _module_uses(spelled, TEMPFILE) == {"tempfile.mkstemp"}
    assert _module_uses(spelled, SHUTIL_COPY) == {"shutil.copytree"}
    assert _util_json(spelled) == {"read_json", "write_json_atomic"}
    assert _calls_by_function(spelled, MKDIR) == {"A.f", "g"}


def test_only_fs_performs_raw_operations() -> None:
    # Rule A: a raw rename/unlink/rmdir/lock/O_CREAT outside _fs is a race the reviewed surfaces do not own.
    allowed = {"_fs.py"} | RUNNER_SIDE | LEGACY_RAW | METHOD_RAW
    offenders = {
        module: sorted(uses) for module in _modules() if module not in allowed and (uses := _raw_uses(_tree(module)))
    }
    assert offenders == {}, f"raw filesystem operations outside _fs.py: {offenders}"


def test_legacy_allowlist_is_tight() -> None:
    # The legacy allowlist may only shrink: an entry whose module is gone or clean must be removed now,
    # or a later regression in that module would pass unnoticed.
    present = set(_modules())
    stale = sorted(module for module in LEGACY_RAW if module not in present or not _raw_uses(_tree(module)))
    assert stale == [], f"remove these from LEGACY_RAW: {stale}"
    # A METHOD_RAW module may hold nothing but pathlib-style calls, and must still hold one.
    misfiled = sorted(
        module
        for module in METHOD_RAW
        if module not in present
        or (uses := _raw_uses(_tree(module))) != _method_calls(_tree(module), RAW_METHODS)
        or not uses
    )
    assert misfiled == [], f"fix these METHOD_RAW entries: {misfiled}"
    assert _raw_uses(_tree("_fs.py")), "the scanner no longer recognises _fs.py's raw operations"


def test_only_kernel_calls_contested_primitives() -> None:
    # Rule B: a contested move decides ownership; only the kernel may make one, so I1-I3 are reviewed in one place.
    offenders = {}
    for module in _modules():
        if module == "_fs.py":
            continue
        used = _names_used(_tree(module), CONTESTED)
        if disallowed := sorted(name for name in used if module not in CONTESTED_CALLERS[name]):
            offenders[module] = disallowed
    assert offenders == {}, f"contested primitives outside the kernel: {offenders}"


def test_no_module_uses_links_or_locks() -> None:
    # Rule C: hard links share inodes between jobs, symlinks invite traversal, and flocks do not hold across
    # NFS nodes; the protocol uses none of them anywhere (ruff TID251 bans them as well).
    offenders = {}
    for module in _modules():
        tree = _tree(module)
        if used := (_module_uses(tree, LINKS) | _names_used(tree, LOCKS)) - LINKS_ALLOWED.get(module, set()):
            offenders[module] = sorted(used)
    assert offenders == {}, f"links or locks in the new modules: {offenders}"


def _allowlist_problems(callers: frozenset[str], uses: dict[str, set[str]]) -> dict[str, object]:
    """Who uses a scanned name without being listed (or while being a protocol module), and stale entries."""

    present = set(_modules())
    return {
        "unlisted": sorted(module for module, used in uses.items() if used and module not in callers),
        "protocol": sorted(module for module in callers if _protocol(module) and module in uses and uses[module]),
        "stale": sorted(module for module in callers if module not in present or not uses.get(module)),
    }


def test_temporaries_and_copies_only_where_listed() -> None:
    # Rule D: tempfile and shutil copies create entries by plain path; each user is listed with its reason, and
    # the lists stay exact.
    tempfile_uses = {module: _module_uses(_tree(module), TEMPFILE) for module in _modules()}
    copy_uses = {module: _module_uses(_tree(module), SHUTIL_COPY) for module in _modules()}
    # Protocol modules listed on purpose: the anonymous seal buffer, the runner-side transaction staging and the
    # private store build; any other protocol module copies through _fs.copy_tree.
    assert _allowlist_problems(TEMPFILE_CALLERS, tempfile_uses) == {
        "unlisted": [],
        "protocol": ["seals.py"],
        "stale": [],
    }
    assert _allowlist_problems(SHUTIL_COPY_CALLERS, copy_uses) == {
        "unlisted": [],
        "protocol": ["_data.py", "_store.py"],
        "stale": [],
    }


def test_protocol_modules_never_use_the_util_json_helpers() -> None:
    # Rule E: control and job files are read bounded and unfollowed, and written by _fs.write_file.
    uses = {module: _util_json(_tree(module)) for module in _modules() if module != "_util.py"}
    assert _allowlist_problems(UTIL_JSON_CALLERS, uses) == {"unlisted": [], "protocol": [], "stale": []}
    assert not [module for module, used in uses.items() if used and _protocol(module)]


def test_protocol_modules_create_directories_only_through_fs() -> None:
    # Rule F: raw mkdir bypasses durable mode and the symlink checks; a launch record dir must survive power loss.
    found = {
        f"{module}:{function}"
        for module in _modules()
        if _protocol(module) and module != "_fs.py"
        for function in _calls_by_function(_tree(module), MKDIR)
    }
    assert found - MKDIR_ALLOWED == set(), f"directories created outside _fs: {sorted(found - MKDIR_ALLOWED)}"
    assert MKDIR_ALLOWED - found == set(), f"remove these from MKDIR_ALLOWED: {sorted(MKDIR_ALLOWED - found)}"
    assert not any(entry.startswith("_kernel.py:") for entry in found)
