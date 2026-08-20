"""Replay of optional transactional-data outcomes."""

import os
import time
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath

from httk.core.digests import sha256_file, tree_digest

from ._util import (
    fsync_directory,
    fsync_file,
    fsync_tree,
    read_json,
    retry_delay,
)
from .errors import FormatError, TransactionError, WorkspaceUnavailableError
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
    raise WorkspaceUnavailableError(f"cannot resolve transaction rename {source} -> {destination}{detail}")


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
    durable: bool = False,
) -> bool:
    """Idempotently apply one published transaction.

    Returns whether the manifest contains operations and therefore advances the
    data generation.

    When *durable* is set, every destination this replay installs and every
    directory whose entries it changes — including the parents that gained or
    lost a name, and the trash a removal moved into — is synchronized before
    this call returns. The manager relies on that ordering: it appends the
    destination state frame and renames the marker out of ``committing`` only
    after replay returns, so a committed transaction is on storage before the
    marker that claims it is.

    :param transaction_dir: Locate the published transaction directory.
    :param data_dir: Locate the workspace data directory to update.
    :param expected_generation: Require this current data generation.
    :param durable: Synchronize installed data before returning.
    :return: Whether the manifest contains operations.
    :raises httk.workflow.errors.FormatError: If the transaction manifest or an operation is invalid.
    :raises httk.workflow.errors.TransactionError: If the generation, source, or destination is invalid.
    :raises httk.workflow.errors.WorkspaceUnavailableError: If an operation cannot be resolved after retries.
    """

    manifest = read_json(transaction_dir / "manifest.json")
    if manifest.get("format") != "httk-workflow-transaction" or manifest.get("format_version") != 2:
        raise FormatError("transaction must use httk-workflow-transaction version 2")
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
    # Parent directories whose entries this replay changed. A durable replay
    # synchronizes each of them once at the end rather than per operation, so a
    # transaction touching many files under one directory pays one directory
    # fsync, not one per file.
    touched_directories: set[Path] = set()
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
            if durable:
                touched_directories.add(destination)
                touched_directories.add(destination.parent)
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
            if durable:
                fsync_file(destination)
                touched_directories.add(destination.parent)
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
            if durable:
                fsync_tree(destination)
                touched_directories.add(destination.parent)
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
            if durable:
                touched_directories.add(destination.parent)
                touched_directories.add(trash.parent)
            continue

        if operation == "replace-tree":
            source = _under(transaction_dir, _relative_path(raw.get("source"), "replace-tree source"))
            expected = str(raw.get("sha256", ""))
            trash = trash_dir / "old"
            if destination.exists() and _digest_matches(destination, expected) and not source.exists():
                continue
            if destination.exists() and not trash.exists():
                _rename_verified(destination, trash)
                if durable:
                    touched_directories.add(trash.parent)
            if not source.is_dir() or not _digest_matches(source, expected):
                raise TransactionError(f"replace-tree source digest mismatch: {path}")
            _rename_verified(source, destination)
            if durable:
                fsync_tree(destination)
                touched_directories.add(destination.parent)
            continue

        raise FormatError(f"unsupported transaction operation: {operation!r}")
    if durable:
        for directory in touched_directories:
            if directory.is_dir():
                fsync_directory(directory)
    return bool(operations)
