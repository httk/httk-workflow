"""Replay of optional transactional-data outcomes."""

import os
import time
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath

from ._util import read_json, retry_delay, sha256_file, tree_digest
from .errors import FormatError, StoreUnavailableError, TransactionError
from .models import validate_label


def _relative_path(value: object, name: str) -> PurePosixPath:
    if not isinstance(value, str) or not value:
        raise FormatError(f"{name} must be a nonempty relative path")
    result = PurePosixPath(value)
    if result.is_absolute() or any(part in {"", ".", ".."} for part in result.parts):
        raise FormatError(f"{name} is not a normalized relative path")
    if any("\x00" in part for part in result.parts):
        raise FormatError(f"{name} contains NUL")
    return result


def _under(root: Path, relative: PurePosixPath) -> Path:
    return root.joinpath(*relative.parts)


def _rename_verified(source: Path, destination: Path, *, replace: bool = False, attempts: int = 7) -> None:
    last_error: OSError | None = None
    for attempt in range(attempts):
        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            if replace:
                os.replace(source, destination)
            else:
                os.rename(source, destination)
        except OSError as exc:
            last_error = exc
        if destination.exists() and not source.exists():
            return
        if source.exists():
            time.sleep(retry_delay(attempt))
            continue
        time.sleep(retry_delay(attempt))
    detail = f": {last_error}" if last_error else ""
    raise StoreUnavailableError(f"cannot resolve transaction rename {source} -> {destination}{detail}")


def _digest_matches(path: Path, expected: str) -> bool:
    if path.is_file():
        return sha256_file(path) == expected
    if path.is_dir():
        return tree_digest(path) == expected
    return False


def replay_transaction(
    transaction_dir: Path,
    data_dir: Path,
    *,
    expected_generation: int,
) -> bool:
    """Idempotently apply one published transaction.

    Returns whether the manifest contains operations and therefore advances the
    data generation.
    """

    manifest = read_json(transaction_dir / "manifest.json")
    if manifest.get("format") != "httk-workflow-transaction" or manifest.get("format_version") != 1:
        raise FormatError("transaction must use httk-workflow-transaction version 1")
    if manifest.get("expected_data_generation") != expected_generation:
        raise TransactionError("transaction expected_data_generation is stale")
    operations = manifest.get("operations")
    if not isinstance(operations, Sequence) or isinstance(operations, (str, bytes)):
        raise FormatError("transaction operations must be an array")
    seen_ids: set[str] = set()
    target_paths: list[PurePosixPath] = []
    for raw in operations:
        if not isinstance(raw, Mapping):
            raise FormatError("transaction operation must be an object")
        operation_id = validate_label(raw.get("id"), "transaction operation id")
        if operation_id in seen_ids:
            raise FormatError(f"duplicate transaction operation id: {operation_id}")
        seen_ids.add(operation_id)
        if raw.get("op") != "make-dir":
            target_paths.append(_relative_path(raw.get("path"), "transaction operation path"))
    for index, left in enumerate(target_paths):
        for right in target_paths[index + 1 :]:
            if left == right or left in right.parents or right in left.parents:
                raise FormatError(f"transaction paths overlap: {left} and {right}")

    data_dir.mkdir(parents=True, exist_ok=True)
    for raw in operations:
        assert isinstance(raw, Mapping)
        operation_id = validate_label(raw.get("id"), "transaction operation id")
        operation = raw.get("op")
        path = _relative_path(raw.get("path"), "transaction operation path")
        destination = _under(data_dir, path)
        trash_dir = transaction_dir / "trash" / operation_id

        if operation == "make-dir":
            destination.mkdir(parents=True, exist_ok=True)
            if not destination.is_dir():
                raise TransactionError(f"make-dir destination is not a directory: {path}")
            continue

        if operation == "put-file":
            source = _under(transaction_dir, _relative_path(raw.get("source"), "put-file source"))
            expected = str(raw.get("sha256", ""))
            if destination.exists() and _digest_matches(destination, expected) and not source.exists():
                continue
            if not source.is_file() or not _digest_matches(source, expected):
                raise TransactionError(f"put-file source digest mismatch: {path}")
            _rename_verified(source, destination, replace=True)
            if not _digest_matches(destination, expected):
                raise TransactionError(f"put-file destination digest mismatch: {path}")
            continue

        if operation == "put-tree":
            source = _under(transaction_dir, _relative_path(raw.get("source"), "put-tree source"))
            expected = str(raw.get("sha256", ""))
            if destination.exists() and _digest_matches(destination, expected) and not source.exists():
                continue
            if destination.exists():
                raise TransactionError(f"put-tree destination already exists: {path}")
            if not source.is_dir() or not _digest_matches(source, expected):
                raise TransactionError(f"put-tree source digest mismatch: {path}")
            _rename_verified(source, destination)
            continue

        if operation == "remove":
            trash = trash_dir / "removed"
            if not destination.exists() and trash.exists():
                continue
            if not destination.exists() and bool(raw.get("missing_ok", False)):
                continue
            if not destination.exists():
                raise TransactionError(f"remove target is missing: {path}")
            _rename_verified(destination, trash)
            continue

        if operation == "replace-tree":
            source = _under(transaction_dir, _relative_path(raw.get("source"), "replace-tree source"))
            expected = str(raw.get("sha256", ""))
            trash = trash_dir / "old"
            if destination.exists() and _digest_matches(destination, expected) and not source.exists():
                continue
            if destination.exists() and not trash.exists():
                _rename_verified(destination, trash)
            if not source.is_dir() or not _digest_matches(source, expected):
                raise TransactionError(f"replace-tree source digest mismatch: {path}")
            _rename_verified(source, destination)
            continue

        raise FormatError(f"unsupported transaction operation: {operation!r}")
    return bool(operations)
