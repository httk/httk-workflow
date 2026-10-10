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
#: The kernel creates directories only through ``_fs.make_dirs``/``open_dir_under`` (durable mode, symlink checks).
KERNEL_MKDIR = {"os": frozenset({"mkdir", "makedirs"})}
CONTESTED = frozenset({"move_once", "publish_dir"})
#: Never in any module: no hard links, no symlinks created, no file locks.
LINKS = {"os": frozenset({"link", "symlink"})}
#: The one exception: ``_fs.copy_tree`` recreates a symlink it copies as a symlink.
LINKS_ALLOWED = {"_fs.py": {"os.symlink"}}
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
    # The daemon ledger's prepared anchors and the ledger itself; its records, like the client's signed request
    # cache, go through _fs.publish_record.
    "publish_dir": frozenset({"_kernel.py", "_daemon_state.py"}),
}


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


def test_kernel_creates_directories_only_through_fs() -> None:
    # Raw mkdir bypasses durable mode and the symlink checks; a launch record dir must survive power loss.
    tree = _tree("_kernel.py")
    assert _module_uses(tree, KERNEL_MKDIR) | _method_calls(tree, frozenset({"mkdir"})) == set()
