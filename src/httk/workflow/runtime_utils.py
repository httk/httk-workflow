"""Safe replacements for small conveniences used by shell runners."""

import ast
import bz2
import gzip
import importlib
import lzma
import math
import os
import shutil
import stat
import string
import tempfile
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

__all__ = [
    "compress_files",
    "decompress_files",
    "evaluate_expression",
    "render_template",
]

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
    """Evaluate a bounded arithmetic expression without Python ``eval``.

    :param expression: Supply the arithmetic expression to evaluate.
    :return: The evaluated scalar result.
    :raises ValueError: If the expression contains an unsupported element.
    """

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
    """Render ``string.Template`` placeholders from an explicit mapping.

    :param template_path: Locate the source template.
    :param output_path: Locate the atomically replaced output file.
    :param values: Supply placeholder values.
    :return: The rendered output path.
    :raises KeyError: If the template names a missing value.
    """

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


def _zstd() -> Any:
    """Return Zstandard support: stdlib ``compression.zstd`` from Python 3.14, else ``backports.zstd``."""

    try:
        return importlib.import_module("compression.zstd")
    except ImportError:
        return importlib.import_module("backports.zstd")


_SUFFIXES = {"bz2": ".bz2", "gz": ".gz", "xz": ".xz", "zst": ".zst"}


def _level_bounds(method: str) -> tuple[int, int, int]:
    """Return ``(minimum, maximum, default)`` compression levels for ``method``."""

    if method == "zst":
        low, high = _zstd().CompressionParameter.compression_level.bounds()
        return low, high, 3
    return {"bz2": (1, 9, 9), "gz": (0, 9, 9), "xz": (0, 9, 6)}[method]


def compress_files(
    paths: Sequence[str | os.PathLike[str]],
    *,
    method: str = "bz2",
    remove_source: bool = False,
    level: int | None = None,
    workers: int = 1,
) -> tuple[Path, ...]:
    """Compress regular files atomically.

    ``zst`` compresses the files one after another, each with ``workers`` libzstd threads;
    the other methods compress up to ``workers`` (at most 16) files concurrently. An
    existing output is replaced atomically and gets the source's permission bits; a gzip
    header records no file name or time stamp. A file named twice is compressed once.

    :param paths: Locate the regular files to compress.
    :param method: Select ``bz2``, ``gz``, ``xz``, or ``zst`` (Zstandard) compression.
    :param remove_source: Remove each source after successful compression.
    :param level: Select the codec level; ``None`` uses the codec default (bz2 9, gz 9, xz 6, zst 3).
    :param workers: Bound the parallelism, at least 1.
    :return: The compressed output paths.
    :raises ValueError: If a method, level, worker count or source file is invalid.
    """

    if method not in _SUFFIXES:
        raise ValueError("compression method must be bz2, gz, xz, or zst")
    if isinstance(workers, bool) or not isinstance(workers, int) or workers < 1:
        raise ValueError(f"compression workers must be a positive integer, got {workers!r}")
    low, high, default = _level_bounds(method)
    if level is not None and (isinstance(level, bool) or not isinstance(level, int) or not low <= level <= high):
        raise ValueError(f"{method} compression level must be an integer in [{low}, {high}], got {level!r}")
    chosen: int = default if level is None else level
    # The same file named twice would be compressed twice, concurrently with workers > 1.
    unique: dict[tuple[int, int], Path] = {}
    for raw in paths:
        source = Path(raw)
        if not source.is_file() or source.is_symlink():
            raise ValueError(f"compression source must be a regular file: {source}")
        identity = source.stat()
        unique.setdefault((identity.st_dev, identity.st_ino), source)
    sources = list(unique.values())

    def open_output(raw: Any) -> Any:
        if method == "zst":
            zstd = _zstd()
            if workers <= 1:
                return zstd.ZstdFile(raw, "w", level=chosen)
            parameter = zstd.CompressionParameter
            return zstd.ZstdFile(raw, "w", options={parameter.compression_level: chosen, parameter.nb_workers: workers})
        if method == "xz":
            return lzma.LZMAFile(raw, "wb", preset=chosen)
        if method == "gz":
            # No file name (it would be the temporary's) and no time stamp in the header.
            return gzip.GzipFile(filename="", mode="wb", fileobj=raw, compresslevel=chosen, mtime=0)
        return bz2.BZ2File(raw, "wb", compresslevel=chosen)

    def compress(source: Path) -> Path:
        destination = source.with_name(source.name + _SUFFIXES[method])
        temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
        try:
            with source.open("rb") as input_stream:
                with temporary.open("wb") as raw, open_output(raw) as output_stream:
                    # An explicit write starts the stream: ZstdFile writes no frame at all for an
                    # empty source unless it saw one, and an empty .zst is not a valid stream.
                    output_stream.write(b"")
                    shutil.copyfileobj(input_stream, output_stream)
                os.chmod(temporary, stat.S_IMODE(os.fstat(input_stream.fileno()).st_mode))
            os.replace(temporary, destination)
            if remove_source:
                source.unlink()
        finally:
            temporary.unlink(missing_ok=True)
        return destination

    if method == "zst" or workers == 1:
        return tuple(compress(source) for source in sources)
    # ponytail: at most 16 files at once; an xz encoder alone holds ~94 MiB, so more threads mostly cost memory.
    with ThreadPoolExecutor(max_workers=min(workers, 16)) as pool:
        return tuple(pool.map(compress, sources))


def decompress_files(
    paths: Sequence[str | os.PathLike[str]],
    *,
    remove_source: bool = False,
) -> tuple[Path, ...]:
    """Decompress ``.bz2``, ``.gz``, ``.xz``, or ``.zst`` files atomically.

    :param paths: Locate the compressed files to decompress.
    :param remove_source: Remove each source after successful decompression.
    :return: The decompressed output paths.
    :raises ValueError: If a source is not a supported regular compressed file.
    :raises FileExistsError: If a decompressed output already exists.
    """

    openers: dict[str, Callable[..., Any]] = {
        ".bz2": bz2.open,
        ".gz": gzip.open,
        ".xz": lzma.open,
        ".zst": lambda path, mode: _zstd().open(path, mode),
    }
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
