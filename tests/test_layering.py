"""The import graph that keeps consumers off the common execution API.

*httk-workflow* has one common execution implementation — the Attempt layer, the
manager, and the modules that own the filesystem protocol — and several
consumers that publish through it: the :mod:`httk.workflow.compat` consumers
(the ``v1`` engine and the CWL, PWD, and jobflow realizations). Simulation-code
support lives outside the distribution and is reached only through the
``httk.core`` code registry. The binding rule is
directional. A consumer may use the common execution API (the root package and
:mod:`httk.workflow.protocol`); the common execution API must never learn which
language or scientific domain uses it, and one consumer must never reach into
the manager, the introspection or CLI internals, or another consumer.

These tests read the import statements of both sides with :mod:`ast` and assert
the edges that would break that rule are absent. They are a static check, not a
framework: an import is a dependency whether or not the line ever runs, so
parsing the source is exactly as strong as the rule and needs no workspace.
"""

import ast
from pathlib import Path

SRC = Path(__file__).parents[1] / "src"
WORKFLOW = SRC / "httk" / "workflow"

#: The modules that make up the one common execution implementation. None of
#: them may name a consumer, because the common layer is blind to its consumers.
COMMON_LAYER = (
    "sdk",
    "runtime",
    "runtime_builders",
    "protocol",
    "manager",
    "workspace",
    "models",
    "_fs",
    "_kernel",
    "_state",
    "_job",
    "_store",
    "_data",
    "_requests",
    "_children",
    "_joins",
    "packages",
    "postprocessing",
)

#: The consumer packages. None may import another. The registry root
#: :mod:`httk.workflow.compat` is common, not a consumer.
CONSUMER_ENGINES = (
    "httk.workflow.compat.v1",
    "httk.workflow.compat.cwl",
    "httk.workflow.compat.pwd",
    "httk.workflow.compat.jobflow",
)

#: Generic machinery a consumer must never reach up into: the manager sees only
#: ordinary jobs, and introspection and the CLI sit above execution.
FORBIDDEN_GENERIC = (
    "httk.workflow.manager",
    "httk.workflow.introspection",
    "httk.workflow.workflow_cli",
)


def _module_name(path: Path) -> str:
    """Return the dotted module name of one file below ``src``."""

    relative = path.relative_to(SRC).with_suffix("")
    parts = relative.parts
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


def _package_of(path: Path) -> str:
    """Return the package a relative import in ``path`` resolves against."""

    module = _module_name(path)
    if path.name == "__init__.py":
        return module
    return module.rpartition(".")[0]


def _imported_modules(path: Path) -> set[str]:
    """Return every absolute module name ``path`` imports, statically."""

    tree = ast.parse(path.read_text(encoding="utf-8"), str(path))
    package = _package_of(path)
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0:
                base = node.module or ""
            else:
                trimmed = package.split(".")
                if node.level > 1:
                    trimmed = trimmed[: -(node.level - 1)]
                base = ".".join(trimmed)
                if node.module:
                    base = f"{base}.{node.module}"
            if base:
                modules.add(base)
    return modules


def _names(module: str, imported: str) -> bool:
    """Return whether ``imported`` is ``module`` or a submodule of it."""

    return imported == module or imported.startswith(f"{module}.")


def test_common_layer_never_imports_a_consumer() -> None:
    """No common-layer module may name a consumer."""

    paths = [WORKFLOW / f"{name}.py" for name in COMMON_LAYER]
    paths.append(WORKFLOW / "compat" / "__init__.py")
    for path in paths:
        offending = sorted(
            imported
            for imported in _imported_modules(path)
            if any(_names(engine, imported) for engine in CONSUMER_ENGINES)
        )
        assert offending == [], f"{_module_name(path)} imports a consumer: {offending}"


def test_compat_registry_is_common_and_lazy() -> None:
    """The registry root does not import any consumer package at import time."""

    path = WORKFLOW / "compat" / "__init__.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), str(path))
    imported = set(_imported_modules(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module == "httk.workflow.compat":
            imported.update(f"{node.module}.{alias.name}" for alias in node.names)
    assert not any(
        _names(f"httk.workflow.compat.{name}", item) for name in ("cwl", "pwd", "jobflow", "v1") for item in imported
    )


def test_the_v1_reader_does_not_load_the_v1_realization() -> None:
    """``httk workflow v1 collect`` imports the reader; only the registry imports the realization."""

    import subprocess
    import sys

    probe = (
        "import sys, httk.workflow.compat.v1, httk.workflow.workflow_cli; "
        "assert 'httk.workflow.compat.v1.realization' not in sys.modules; "
        "from httk.workflow import compat; "
        "assert compat.language('httk-v1') is sys.modules['httk.workflow.compat.v1.realization'].LANGUAGE"
    )
    subprocess.run([sys.executable, "-c", probe], check=True, env={"PYTHONPATH": str(SRC)})
    for path in (WORKFLOW / "compat" / "v1").glob("*.py"):
        if path.name != "realization.py":
            assert not any(_names("httk.workflow.compat.v1.realization", item) for item in _imported_modules(path)), (
                f"{_module_name(path)} imports the v1 realization"
            )


def _consumer_modules() -> list[Path]:
    """Return every consumer module whose imports the rule constrains."""

    return sorted(path for path in (WORKFLOW / "compat").rglob("*.py") if path.parent != WORKFLOW / "compat")


def _consumer_owner(module: str) -> str | None:
    """Return the consumer owning *module*."""

    return next((engine for engine in CONSUMER_ENGINES if _names(engine, module)), None)


def test_consumers_do_not_reach_into_generic_or_each_other() -> None:
    """A consumer imports the common API and its own package, nothing sideways."""

    for path in _consumer_modules():
        module = _module_name(path)
        own = _consumer_owner(module)
        imported = _imported_modules(path)
        for target in sorted(imported):
            for generic in FORBIDDEN_GENERIC:
                assert not _names(generic, target), f"{module} reaches into {target}"
            for engine in CONSUMER_ENGINES:
                if engine != own:
                    assert not _names(engine, target), f"{module} imports the {engine} consumer"


def test_generic_modules_hold_no_vasp_knowledge() -> None:
    """The generic scaffold, manager, and shell bridge name no simulation code.

    Workflows reach the generic scaffold only through its provider registry, and
    code support reaches the manager and the shell bridge only through the
    ``httk.core`` code registry, so none of them carries a hardcoded VASP workflow
    table, runner, bridge, or Bash API path. The scaffold keeps only the POSCAR
    file-naming conventions of its structure discovery, so it is checked by
    token; the manager and the bridge must not mention VASP at all.
    """

    source = (WORKFLOW / "scaffold.py").read_text(encoding="utf-8")
    for token in (
        "_PACKAGED",
        "PACKAGED_TEMPLATES",
        "httk.vasp",
        "httk.codes",
        "vasp-",
        "vasp_",
        "VASP_",
        "WorkflowProvider(",
    ):
        assert token not in source, f"scaffold must not name the VASP domain: {token!r}"
    for name in ("manager.py", "_shell_bridge.py"):
        assert "vasp" not in (WORKFLOW / name).read_text(encoding="utf-8").lower(), f"{name} names VASP"
    for name in ("scaffold.py", "manager.py", "_shell_bridge.py"):
        for imported in _imported_modules(WORKFLOW / name):
            assert not _names("httk.codes", imported), f"{name} imports {imported}"
            assert not any(_names(engine, imported) for engine in CONSUMER_ENGINES), f"{name} imports {imported}"
