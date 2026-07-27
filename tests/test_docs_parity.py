"""The authoring parity table in ``docs/sdk_parity.md`` is enforced, not aspired to.

The table is the single documented source of truth for the authoring surface: one
row per operation, spelled in Python and in Bash. This module parses it and holds
it to three properties, so that the table cannot drift away from the code in
either direction:

1. every Python member the table lists really exists on :class:`Runner`,
   :class:`Attempt`, or a type exported from :mod:`httk.workflow`;
2. every Bash function the table lists is really defined in the packaged
   ``shell/httk-workflow.sh``;
3. every public member of :class:`Runner` and :class:`Attempt`, and every
   function of that shell library, really appears in the table.

The third one is what makes the table a *reference* rather than a sample: adding
an authoring feature to the code without documenting it fails here.
"""

import ast
import dataclasses
import importlib
import inspect
import re
from pathlib import Path

import httk.workflow
from httk.workflow import Attempt, Runner

_DOCS = Path(__file__).resolve().parent.parent / "docs" / "sdk_parity.md"
_SHELL = Path(httk.workflow.__file__).with_name("shell") / "httk-workflow.sh"

#: The table lives under this heading; everything before it is prose.
_TABLE_HEADING = "## The table"

_CODE_SPAN = re.compile(r"`([^`]+)`")
_SHELL_FUNCTION = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)\(\)\s*\{", re.MULTILINE)

#: Internals of the shell library itself, which are not authoring surface: the
#: bridge invocation, the two registration checks, the describe printer, and the
#: ERR/EXIT traps that turn a dying handler into a breadcrumb. A runner never
#: calls any of them, so documenting them would document the implementation
#: rather than the contract.
_UNDOCUMENTED_SHELL = frozenset(
    {
        "_httk_workflow_bridge",
        "_httk_workflow_check_registration",
        "_httk_workflow_describe",
        "_httk_workflow_has_step",
        "_httk_workflow_step_exit",
        "_httk_workflow_trace",
    }
)

#: Public members of Runner or Attempt that are deliberately absent from the
#: table. Empty on purpose: every public member of either class is authoring
#: surface, and anything that is not belongs behind an underscore instead.
_UNDOCUMENTED_PYTHON: frozenset[str] = frozenset()


def _table_rows() -> list[tuple[str, ...]]:
    """Return the cells of every data row of the one parity table."""

    text = _DOCS.read_text(encoding="utf-8")
    assert _TABLE_HEADING in text, f"{_DOCS} no longer has a {_TABLE_HEADING!r} section"
    body = text.split(_TABLE_HEADING, 1)[1]
    rows: list[tuple[str, ...]] = []
    for line in body.splitlines():
        stripped = line.strip()
        if not stripped.startswith("|"):
            if rows:
                # The table is one contiguous block; the first non-row line after
                # it ends it, so trailing prose is never parsed as data.
                break
            continue
        cells = tuple(cell.strip() for cell in stripped.strip("|").split("|"))
        if len(cells) != 4 or set(cells[0]) <= {"-", ":"}:
            continue
        if cells[0] == "Python":
            continue
        rows.append(cells)
    return rows


def _names(cell: str) -> list[str]:
    """Return the API names a table cell lists.

    Only code spans are normative, and only their first word: a cell may carry
    the option that selects the row's behaviour (``httk_workflow_spawn --step``)
    without that option becoming part of the name.
    """

    return [span.split()[0] for span in _CODE_SPAN.findall(cell) if span.split()]


def _instance_attributes(cls: type) -> frozenset[str]:
    """Return the ``self.X = ...`` names one class binds in its ``__init__``.

    Attributes bound there are as much part of the surface as methods are, and
    they are invisible to :func:`dir`, so the class source is read for them.
    """

    tree = ast.parse(inspect.getsource(cls))
    class_node = tree.body[0]
    assert isinstance(class_node, ast.ClassDef)
    found: set[str] = set()
    for node in class_node.body:
        if not isinstance(node, ast.FunctionDef) or node.name != "__init__":
            continue
        for statement in ast.walk(node):
            if not isinstance(statement, ast.Assign):
                continue
            for target in statement.targets:
                if (
                    isinstance(target, ast.Attribute)
                    and isinstance(target.value, ast.Name)
                    and target.value.id == "self"
                ):
                    found.add(target.attr)
    return frozenset(found)


def _surface(cls: type) -> frozenset[str]:
    """Return every public member of *cls*: methods, properties, attributes."""

    members = set(vars(cls)) | set(_instance_attributes(cls))
    if dataclasses.is_dataclass(cls):
        members |= {field.name for field in dataclasses.fields(cls)}
    return frozenset(name for name in members if not name.startswith("_"))


def _shell_functions() -> frozenset[str]:
    return frozenset(_SHELL_FUNCTION.findall(_SHELL.read_text(encoding="utf-8")))


def _python_names() -> list[str]:
    return [name for row in _table_rows() for name in _names(row[0])]


def _bash_names() -> list[str]:
    return [name for row in _table_rows() for name in _names(row[1])]


def test_the_table_has_rows() -> None:
    """A parser that silently matched nothing would make every check vacuous."""

    rows = _table_rows()
    assert len(rows) > 40, f"only {len(rows)} parity rows were parsed from {_DOCS}"
    assert all(any(cell not in {"", "—"} for cell in row) for row in rows)


def _reach(obj: object, part: str) -> object | None:
    """Return the attribute *part* of *obj*, importing it as a submodule if need be."""

    reached = getattr(obj, part, None)
    if reached is None:
        module_name = getattr(obj, "__name__", None)
        if module_name is not None:
            try:
                reached = importlib.import_module(f"{module_name}.{part}")
            except ModuleNotFoundError:
                reached = None
    return reached


def _resolve(dotted: str) -> bool:
    """Report whether *dotted* resolves by attribute walk from ``httk.workflow``.

    A name is either a bare root export (``Runner``), a member of a root class
    (``Attempt.run``), or a name reached through a named submodule
    (``protocol.JobSpec``, ``runtime_utils.render_template``). A submodule that is
    not yet imported is imported on demand, so the table may spell a name at its
    real home without the test importing every submodule up front. The final
    component is checked against the owner's whole surface, so an instance
    attribute a class binds in ``__init__`` (``Runner.workflow``) resolves too.
    """

    parts = dotted.split(".")
    owner: object = httk.workflow
    for part in parts[:-1]:
        owner = _reach(owner, part)
        if owner is None:
            return False
    last = parts[-1]
    if _reach(owner, last) is not None:
        return True
    return isinstance(owner, type) and last in _surface(owner)


def test_every_documented_python_member_exists() -> None:
    """(a) Nothing in the Python column is a name that was renamed or removed."""

    for name in _python_names():
        assert _resolve(name), f"{name!r} does not resolve from httk.workflow, but {_DOCS.name} lists it"


def test_every_documented_bash_function_is_defined() -> None:
    """(b) Nothing in the Bash column is a function the library does not define."""

    defined = _shell_functions()
    for name in _bash_names():
        assert name in defined, f"{_SHELL.name} defines no {name!r}, but {_DOCS.name} lists it"


def test_the_whole_python_authoring_surface_is_documented() -> None:
    """(c) No public member of Runner or Attempt is missing from the table."""

    documented = set(_python_names())
    missing: list[str] = []
    for cls in (Runner, Attempt):
        for member in sorted(_surface(cls)):
            qualified = f"{cls.__name__}.{member}"
            if qualified not in documented and qualified not in _UNDOCUMENTED_PYTHON:
                missing.append(qualified)
    assert not missing, f"undocumented authoring surface: {', '.join(missing)}"


def test_the_whole_bash_authoring_surface_is_documented() -> None:
    """(c) No function of the packaged Bash library is missing from the table."""

    documented = set(_bash_names())
    missing = sorted(_shell_functions() - documented - _UNDOCUMENTED_SHELL)
    assert not missing, f"undocumented Bash authoring surface: {', '.join(missing)}"
