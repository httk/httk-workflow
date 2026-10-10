"""The package root re-exports its public surface lazily, keeping the packaged remote adapter cheap to start."""

import ast
import subprocess
import sys
from pathlib import Path

import pytest

import httk.workflow

_SRC = str(Path(httk.workflow.__file__).resolve().parents[2])
_STUB = Path(httk.workflow.__file__).with_suffix(".pyi")


@pytest.mark.parametrize("name", sorted(httk.workflow.__all__))
def test_every_root_export_resolves(name: str) -> None:
    # ``__import__`` with a fromlist is what ``from httk.workflow import <name>`` executes.
    imported = getattr(__import__("httk.workflow", fromlist=[name]), name)
    assert imported is getattr(httk.workflow, name)
    assert name in dir(httk.workflow)


def test_unknown_root_attribute_raises() -> None:
    with pytest.raises(AttributeError, match="no_such_name"):
        _ = httk.workflow.no_such_name  # type: ignore[attr-defined]  # pyright: ignore[reportAttributeAccessIssue]


def test_adapter_runtime_import_stays_lean() -> None:
    probe = (
        "import sys, httk.workflow.adapter_runtime; "
        "print(sorted(m for m in ('numpy', 'httk.core', 'httk.workflow.workspace') if m in sys.modules))"
    )
    result = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, check=True, env={"PYTHONPATH": _SRC}
    )
    assert result.stdout.strip() == "[]"


def test_stub_mirrors_the_lazy_root() -> None:
    # The stub is the static view (type checkers, API reference); the runtime map must say the same.
    stub = ast.parse(_STUB.read_text(encoding="utf-8"))
    imported = {
        alias.name: node.module
        for node in stub.body
        if isinstance(node, ast.ImportFrom) and node.level == 1
        for alias in node.names
    }
    assert imported == vars(httk.workflow)["_LAZY_EXPORTS"]  # private: not part of the stub
    (stub_all,) = (
        node.value
        for node in stub.body
        if isinstance(node, ast.Assign) and [ast.unparse(target) for target in node.targets] == ["__all__"]
    )
    assert ast.literal_eval(stub_all) == httk.workflow.__all__
    assert ast.get_docstring(stub, clean=False) == httk.workflow.__doc__
