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
CONTESTED = frozenset({"move_once", "publish_dir"})
#: Never in the new modules: no hard links, no symlinks created, no file locks.
LINKS = {"os": frozenset({"link", "symlink"})}
LOCKS = frozenset({"flock", "lockf"})
NEW_MODULES = ("_death.py", "_fs.py", "_kernel.py", "_state.py", "_store.py")

#: The runner side acts only inside its own attempt directory (P1).
RUNNER_SIDE = frozenset({"runtime_builders.py", "sdk.py"})
# legacy: shrinks per phase; never add to it.
LEGACY_RAW = frozenset(
    {
        "_adoption.py",
        "_confine.py",
        "_confine_rank.py",
        "_daemon_client.py",
        "_daemon_keys.py",
        "_daemon_mailbox.py",
        "_daemon_setup.py",
        "_daemon_state.py",
        "_exchange.py",
        "_jobdir.py",
        "_launch_client.py",
        "_logging.py",
        "_manager_launches.py",
        "_manager_requests.py",
        "_runner_builds.py",
        "_sealing.py",
        "_txn.py",
        "_util.py",
        "adapters.py",
        "compat/cwl/cwl_runner.py",
        "compat/jobflow/jobflow_runner.py",
        "compat/v1/v1_runner.py",
        "gc.py",
        "hygiene.py",
        "introspection/_debug.py",
        "launchers.py",
        "manifests.py",
        "registry.py",
        "removal.py",
        "runtime_utils.py",
        "scaffold.py",
        "transactions.py",
        "workflow_cli/_workspace.py",
        "workspace.py",
    }
)
#: Who may call each contested primitive besides its definition in ``_fs.py``.
CONTESTED_CALLERS: Mapping[str, frozenset[str]] = {
    "move_once": frozenset({"_kernel.py"}),
    # The daemon's own single-process ledger; it may not use publish_dir yet.
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


def _raw_uses(tree: ast.Module) -> set[str]:
    return _module_uses(tree, RAW_CALLS) | _names_used(tree, RAW_FLAG)


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
    _fs.move_once(s, d)
    return os.O_WRONLY | os.O_CREAT
'''
    )
    assert _raw_uses(tree) == {"os.replace", "shutil.rmtree", "fcntl.flock", "O_CREAT"}
    assert _names_used(tree, CONTESTED) == {"move_once", "publish_dir"}
    assert _module_uses(ast.parse("import os\nos.link(a, b)\nos.symlink(a, b)\n"), LINKS) == {"os.link", "os.symlink"}


def test_only_fs_performs_raw_operations() -> None:
    # Rule A: a raw rename/unlink/rmdir/lock/O_CREAT outside _fs is a race the reviewed surfaces do not own.
    allowed = {"_fs.py"} | RUNNER_SIDE | LEGACY_RAW
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


def test_new_modules_use_no_links_or_locks() -> None:
    # Rule C: hard links share inodes between jobs, symlinks invite traversal, and flocks do not hold across
    # NFS nodes; the redesign's protocol uses none of them.
    offenders = {}
    for module in NEW_MODULES:
        tree = _tree(module)
        if used := _module_uses(tree, LINKS) | _names_used(tree, LOCKS):
            offenders[module] = sorted(used)
    assert offenders == {}, f"links or locks in the new modules: {offenders}"
