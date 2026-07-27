"""The import graph that keeps consumers off the common execution API.

*httk-workflow* has one common execution implementation — the Attempt layer, the
manager, and the modules that own the filesystem protocol — and several
consumers that publish through it: the VASP domain package and the ``v1``,
``cwl`` and ``pwd`` compatibility engines. The binding rule is directional. A
consumer may use the common execution API (the root package and
:mod:`httk.workflow.protocol`); the common execution API must never learn which
compat format or scientific domain uses it, and one consumer must never reach
into the manager, the introspection or CLI internals, or another consumer.

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
    "journal",
    "transactions",
)

#: The consumer packages. Each is a domain or a compat language; none may import
#: another, and only VASP-through-common is the sanctioned cross edge.
CONSUMER_ENGINES = (
    "httk.workflow.vasp",
    "httk.workflow.compat.v1",
    "httk.workflow.compat.cwl",
    "httk.workflow.compat.pwd",
)

#: Generic machinery a consumer must never reach up into: the manager sees only
#: ordinary jobs, and introspection and the CLI sit above execution.
FORBIDDEN_GENERIC = (
    "httk.workflow.manager",
    "httk.workflow.introspection",
    "httk.workflow.workflow_cli",
)

#: The workspace lookups that rescan every state kind when the in-memory index
#: misses. They resolve any job from its id or key alone, but at the price of a
#: whole-workspace scan — exactly the unbounded fallback the streaming scheduler
#: must never take inside a tick. The manager probes each marker's exact
#: placement instead (``find_marker_at``); these two names belong to interactive
#: and CLI resolution only.
FORBIDDEN_SCHEDULING_LOOKUPS = frozenset({"find_marker_by_id", "find_markers"})

#: Explicit, justified exceptions to the rule above, keyed by
#: ``(enclosing function, attribute)``. It is empty: the manager carries no
#: interactive resolver, so every marker it resolves is resolved by placement.
SCHEDULING_LOOKUP_ALLOW: frozenset[tuple[str, str]] = frozenset()


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
    """No common-layer module may name VASP or any compat engine."""

    for name in COMMON_LAYER:
        path = WORKFLOW / f"{name}.py"
        offending = sorted(
            imported
            for imported in _imported_modules(path)
            if _names("httk.workflow.vasp", imported) or _names("httk.workflow.compat", imported)
        )
        assert offending == [], f"{name} imports a consumer: {offending}"


def _consumer_modules() -> list[Path]:
    """Return every consumer module whose imports the rule constrains.

    The ``httk-v1-taskmanager`` alias (``compat/v1/cli.py``) is deliberately
    excluded: it is not an execution engine but a thin console entry point of the
    CLI layer, and building the canonical ``httk workflow v1`` subcommands out of
    the very declarations the canonical tree uses is exactly its job.
    """

    paths: list[Path] = []
    for path in sorted((WORKFLOW / "vasp").rglob("*.py")):
        paths.append(path)
    for path in sorted((WORKFLOW / "compat").rglob("*.py")):
        if path.name == "cli.py":
            continue
        paths.append(path)
    return paths


def test_consumers_do_not_reach_into_generic_or_each_other() -> None:
    """A consumer imports the common API and its own package, nothing sideways."""

    for path in _consumer_modules():
        module = _module_name(path)
        own = next((engine for engine in CONSUMER_ENGINES if _names(engine, module)), None)
        imported = _imported_modules(path)
        for target in sorted(imported):
            for generic in FORBIDDEN_GENERIC:
                assert not _names(generic, target), f"{module} reaches into {target}"
            for engine in CONSUMER_ENGINES:
                if engine != own:
                    assert not _names(engine, target), f"{module} imports the {engine} consumer"


def test_the_manager_never_falls_back_to_a_whole_workspace_lookup() -> None:
    """No scheduling pass may resolve a marker by a whole-workspace scan.

    ``find_marker_by_id`` and ``find_markers`` rescan every state kind when the
    marker index misses — the unbounded fallback Phase 13 removed from the hot
    path. This reads the manager's syntax tree and asserts neither name is called
    anywhere in it, save the explicit (and currently empty) allow-list, so a
    future pass cannot quietly reintroduce the fallback that does not scale.
    """

    tree = ast.parse((WORKFLOW / "manager.py").read_text(encoding="utf-8"), "manager.py")
    offending: list[str] = []
    for function in ast.walk(tree):
        if not isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for node in ast.walk(function):
            if isinstance(node, ast.Attribute) and node.attr in FORBIDDEN_SCHEDULING_LOOKUPS:
                if (function.name, node.attr) in SCHEDULING_LOOKUP_ALLOW:
                    continue
                offending.append(f"{function.name} calls {node.attr} (line {node.lineno})")
    assert offending == [], f"manager.py reaches for a whole-workspace lookup: {sorted(set(offending))}"


def test_scaffold_holds_no_vasp_knowledge() -> None:
    """The generic scaffold registers no domain: a domain self-registers instead.

    Templates reach the generic scaffold only through its provider registry, so
    the module carries no hardcoded VASP template table, no VASP runner, workflow
    or template identifier, and no import of the science that owns them. What a
    provider supplies at import is data; the scaffold never names the domain, and
    it registers nothing itself.
    """

    source = (WORKFLOW / "scaffold.py").read_text(encoding="utf-8")
    for token in (
        "_PACKAGED",
        "PACKAGED_TEMPLATES",
        "httk.vasp",
        "vasp-relax",
        "vasp_relax",
        "vasp-static",
        "vasp_static",
        "TemplateProvider(",
    ):
        assert token not in source, f"scaffold must not name the VASP domain: {token!r}"
    for imported in _imported_modules(WORKFLOW / "scaffold.py"):
        assert not _names("httk.workflow.vasp", imported)
        assert not _names("httk.workflow.compat", imported)
