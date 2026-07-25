"""Safe replacements for small conveniences used by shell runners."""

import ast
import bz2
import gzip
import lzma
import math
import os
import shutil
import string
import tempfile
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

_FUNCTIONS: dict[str, Callable[..., Any]] = {
    "abs": abs,
    "ceil": math.ceil,
    "floor": math.floor,
    "int": int,
    "max": max,
    "min": min,
    "sqrt": math.sqrt,
}
_BINARY: dict[type[ast.AST], Callable[[Any, Any], Any]] = {
    ast.Add: lambda left, right: left + right,
    ast.Sub: lambda left, right: left - right,
    ast.Mult: lambda left, right: left * right,
    ast.Div: lambda left, right: left / right,
    ast.FloorDiv: lambda left, right: left // right,
    ast.Mod: lambda left, right: left % right,
    ast.Pow: lambda left, right: left**right,
}
_COMPARE: dict[type[ast.AST], Callable[[Any, Any], bool]] = {
    ast.Eq: lambda left, right: left == right,
    ast.NotEq: lambda left, right: left != right,
    ast.Lt: lambda left, right: left < right,
    ast.LtE: lambda left, right: left <= right,
    ast.Gt: lambda left, right: left > right,
    ast.GtE: lambda left, right: left >= right,
}


def evaluate_expression(expression: str) -> int | float | bool:
    """Evaluate a bounded arithmetic expression without Python ``eval``."""

    tree = ast.parse(expression, mode="eval")

    def evaluate(node: ast.AST) -> int | float | bool:
        if isinstance(node, ast.Constant) and isinstance(node.value, (int, float, bool)):
            return node.value
        if isinstance(node, ast.BinOp) and type(node.op) in _BINARY:
            return _BINARY[type(node.op)](evaluate(node.left), evaluate(node.right))
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub, ast.Not)):
            value = evaluate(node.operand)
            if isinstance(node.op, ast.UAdd):
                return +value
            if isinstance(node.op, ast.USub):
                return -value
            return not value
        if isinstance(node, ast.BoolOp) and isinstance(node.op, (ast.And, ast.Or)):
            values = [bool(evaluate(item)) for item in node.values]
            return all(values) if isinstance(node.op, ast.And) else any(values)
        if isinstance(node, ast.Compare):
            left = evaluate(node.left)
            for operator, raw_right in zip(node.ops, node.comparators, strict=True):
                if type(operator) not in _COMPARE:
                    raise ValueError("unsupported comparison")
                right = evaluate(raw_right)
                if not _COMPARE[type(operator)](left, right):
                    return False
                left = right
            return True
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in _FUNCTIONS
            and not node.keywords
        ):
            return _FUNCTIONS[node.func.id](*(evaluate(item) for item in node.args))
        raise ValueError(f"unsupported expression element: {type(node).__name__}")

    return evaluate(tree.body)


def render_template(
    template_path: str | os.PathLike[str],
    output_path: str | os.PathLike[str],
    values: Mapping[str, object],
) -> Path:
    """Render ``string.Template`` placeholders from an explicit mapping."""

    source = Path(template_path).read_text(encoding="utf-8")
    rendered = string.Template(source).substitute({key: str(value) for key, value in values.items()})
    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(prefix=f".{destination.name}.", dir=destination.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(rendered)
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    return destination


def compress_files(
    paths: Sequence[str | os.PathLike[str]],
    *,
    method: str = "bz2",
    remove_source: bool = False,
) -> tuple[Path, ...]:
    """Compress regular files atomically with a stdlib codec."""

    suffixes = {"bz2": ".bz2", "gz": ".gz", "xz": ".xz"}
    openers: dict[str, Callable[..., Any]] = {"bz2": bz2.open, "gz": gzip.open, "xz": lzma.open}
    if method not in suffixes:
        raise ValueError("compression method must be bz2, gz, or xz")
    results: list[Path] = []
    for raw in paths:
        source = Path(raw)
        if not source.is_file() or source.is_symlink():
            raise ValueError(f"compression source must be a regular file: {source}")
        destination = source.with_name(source.name + suffixes[method])
        temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
        try:
            with source.open("rb") as input_stream, openers[method](temporary, "wb") as output_stream:
                shutil.copyfileobj(input_stream, output_stream)
            os.replace(temporary, destination)
            if remove_source:
                source.unlink()
        finally:
            temporary.unlink(missing_ok=True)
        results.append(destination)
    return tuple(results)


def decompress_files(
    paths: Sequence[str | os.PathLike[str]],
    *,
    remove_source: bool = False,
) -> tuple[Path, ...]:
    """Decompress ``.bz2``, ``.gz``, or ``.xz`` files atomically."""

    openers: dict[str, Callable[..., Any]] = {".bz2": bz2.open, ".gz": gzip.open, ".xz": lzma.open}
    results: list[Path] = []
    for raw in paths:
        source = Path(raw)
        opener = openers.get(source.suffix.lower())
        if opener is None or not source.is_file() or source.is_symlink():
            raise ValueError(f"unsupported compressed regular file: {source}")
        destination = source.with_suffix("")
        if destination.exists():
            raise FileExistsError(destination)
        temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
        try:
            with opener(source, "rb") as input_stream, temporary.open("wb") as output_stream:
                shutil.copyfileobj(input_stream, output_stream)
            os.replace(temporary, destination)
            if remove_source:
                source.unlink()
        finally:
            temporary.unlink(missing_ok=True)
        results.append(destination)
    return tuple(results)
