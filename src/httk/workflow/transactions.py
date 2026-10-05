"""Replay of optional transactional-data outcomes.

A replay runs on descriptors. The manager pins the transaction directory and
the data directory without following links, and every component of every
operation path below them is opened ``O_DIRECTORY|O_NOFOLLOW``, so a symlink or
special file planted anywhere on an operation's source or destination path
fails the replay instead of redirecting a rename, and nothing outside the two
pinned directories is touched. The public :func:`replay_transaction` is the
runner's replay into its own workdir and follows symlinks.
"""

import contextlib
import logging
import os
import stat
import time
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from ._jobdir import CONTROL_DOCUMENT_LIMIT, JobDirectory, JobDirectoryError
from ._util import retry_delay
from .errors import FormatError, TransactionError, WorkspaceUnavailableError
from .models import validate_label

_LOGGER = logging.getLogger(__name__)


def _relative_path(value: object, name: str) -> PurePosixPath:
    if not isinstance(value, str) or not value:
        raise FormatError(f"{name} must be a nonempty relative path")
    result = PurePosixPath(value)
    if result.is_absolute() or any(part in {"", ".", ".."} for part in result.parts):
        raise FormatError(f"{name} is not a normalized relative path")
    if any("\x00" in part for part in result.parts):
        raise FormatError(f"{name} contains NUL")
    return result


@dataclass(frozen=True)
class _Location:
    """One entry, named within a pinned parent directory."""

    parent: JobDirectory
    name: str

    @property
    def shown(self) -> Path:
        return self.parent.path / self.name

    def exists(self) -> bool:
        return self.parent.stat(self.name) is not None


def _rename_verified(source: _Location, destination: _Location, *, replace: bool = False, attempts: int = 7) -> None:
    last_error: OSError | None = None
    rename = os.replace if replace else os.rename
    for attempt in range(attempts):
        try:
            rename(source.name, destination.name, src_dir_fd=source.parent.fd, dst_dir_fd=destination.parent.fd)
        except OSError as exc:
            last_error = exc
        if destination.exists() and not source.exists():
            return
        time.sleep(retry_delay(attempt))
    detail = f": {last_error}" if last_error else ""
    raise WorkspaceUnavailableError(f"cannot resolve transaction rename {source.shown} -> {destination.shown}{detail}")


def _entry(root: JobDirectory, relative: PurePosixPath, *, follow: bool) -> os.stat_result | None:
    """Return an operation entry's status, refusing a symlink or special file unless *follow*.

    A following replay (one given paths) keeps the original semantics: links
    are followed, and anything that is neither a file nor a directory merely
    never matches a digest.
    """

    information = root.stat(relative)
    if information is None or follow:
        return information
    if not (stat.S_ISREG(information.st_mode) or stat.S_ISDIR(information.st_mode)):
        raise JobDirectoryError(f"{root.path.joinpath(*relative.parts)} is a symlink or special file")
    return information


def _digest_matches(root: JobDirectory, relative: PurePosixPath, information: os.stat_result, expected: str) -> bool:
    if stat.S_ISREG(information.st_mode):
        return root.digest_file(relative) == expected
    if stat.S_ISDIR(information.st_mode):
        return root.digest_tree(relative) == expected
    return False


@contextlib.contextmanager
def _parent(root: JobDirectory, relative: PurePosixPath, *, create: bool = False) -> Iterator[_Location]:
    handle, name = root.open_parent(relative, create=create)
    with handle:
        yield _Location(handle, name)


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

    This is the replay a runner applies to a batch in its own workdir: it acts
    in a tree the runner owns and follows symlinks, so a workdir entry such as
    ``scratch -> /scratch/...`` works. The manager replays a job's published
    transaction through descriptors pinned without following links instead.

    :param transaction_dir: The published transaction directory.
    :param data_dir: The data directory to update, created when missing.
    :param expected_generation: Require this current data generation.
    :param durable: Synchronize installed data before returning.
    :return: Whether the manifest contains operations.
    :raises httk.workflow.errors.FormatError: If the transaction manifest or an operation is invalid.
    :raises httk.workflow.errors.TransactionError: If the generation, source, or destination is invalid.
    :raises httk.workflow.errors.WorkspaceUnavailableError: If an operation cannot be resolved after retries.
    """

    data_dir.mkdir(parents=True, exist_ok=True)
    with (
        JobDirectory.at(transaction_dir, follow_symlinks=True) as transaction,
        JobDirectory.at(data_dir, follow_symlinks=True) as data,
    ):
        return _replay_pinned(transaction, data, expected_generation=expected_generation, durable=durable, follow=True)


def _replay_pinned(
    transaction: JobDirectory,
    data: JobDirectory,
    *,
    expected_generation: int,
    durable: bool = False,
    follow: bool = False,
) -> bool:
    """Replay one transaction between two pinned directories; the manager's entry point.

    With the default ``follow=False`` every component below the two handles
    is opened without following links, and a symlink or special file on any
    source or destination path, including the final name, fails the replay
    with :class:`~httk.workflow._jobdir.JobDirectoryError` rather than being
    followed or replaced.
    """

    manifest = transaction.read_json("manifest.json", CONTROL_DOCUMENT_LIMIT)
    operations = _validated_operations(manifest, expected_generation)
    _replay(transaction, data, operations, durable=durable, follow=follow)
    return bool(operations)


def _validated_operations(manifest: Mapping[str, object], expected_generation: int) -> Sequence[Mapping[str, object]]:
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
    return operations


def _replay(
    transaction: JobDirectory,
    data: JobDirectory,
    operations: Sequence[Mapping[str, object]],
    *,
    durable: bool,
    follow: bool,
) -> None:
    # Directories whose entries this replay changed, as (root, relative path)
    # pairs. A durable replay synchronizes each of them once at the end rather
    # than per operation, so a transaction touching many files under one
    # directory pays one directory fsync, not one per file.
    touched_directories: set[tuple[str, PurePosixPath]] = set()
    roots = {"data": data, "transaction": transaction}
    for raw in operations:
        operation_id = validate_label(raw.get("id"), "transaction operation id")
        operation = raw.get("op")
        path = _relative_path(raw.get("path"), "transaction operation path")
        trash_root = PurePosixPath("trash", operation_id)

        if operation == "make-dir":
            existing = _entry(data, path, follow=follow)
            if existing is not None and not stat.S_ISDIR(existing.st_mode):
                raise TransactionError(f"make-dir destination is not a directory: {path}")
            data.directory(path, create=True).close()
            if durable:
                touched_directories.update({("data", path), ("data", path.parent)})
            continue

        if operation == "put-file":
            source = _relative_path(raw.get("source"), "put-file source")
            expected = str(raw.get("sha256", ""))
            destination_state = _entry(data, path, follow=follow)
            source_state = _entry(transaction, source, follow=follow)
            if (
                destination_state is not None
                and _digest_matches(data, path, destination_state, expected)
                and source_state is None
            ):
                continue
            if (
                source_state is None
                or not stat.S_ISREG(source_state.st_mode)
                or not _digest_matches(transaction, source, source_state, expected)
            ):
                raise TransactionError(f"put-file source digest mismatch: {path}")
            with _parent(transaction, source) as origin, _parent(data, path, create=True) as target:
                _rename_verified(origin, target, replace=True)
            destination_state = _entry(data, path, follow=follow)
            if destination_state is None or not _digest_matches(data, path, destination_state, expected):
                raise TransactionError(f"put-file destination digest mismatch: {path}")
            if durable:
                descriptor = data.open_read(path)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
                touched_directories.add(("data", path.parent))
            continue

        if operation == "put-tree":
            source = _relative_path(raw.get("source"), "put-tree source")
            expected = str(raw.get("sha256", ""))
            destination_state = _entry(data, path, follow=follow)
            source_state = _entry(transaction, source, follow=follow)
            if (
                destination_state is not None
                and _digest_matches(data, path, destination_state, expected)
                and source_state is None
            ):
                continue
            if destination_state is not None:
                raise TransactionError(f"put-tree destination already exists: {path}")
            if (
                source_state is None
                or not stat.S_ISDIR(source_state.st_mode)
                or not _digest_matches(transaction, source, source_state, expected)
            ):
                raise TransactionError(f"put-tree source digest mismatch: {path}")
            with _parent(transaction, source) as origin, _parent(data, path, create=True) as target:
                _rename_verified(origin, target)
            if durable:
                data.fsync_tree(path)
                touched_directories.add(("data", path.parent))
            continue

        if operation == "remove":
            trash = trash_root / "removed"
            if _entry(data, path, follow=follow) is None:
                if transaction.stat(trash) is not None or bool(raw.get("missing_ok", False)):
                    continue
                raise TransactionError(f"remove target is missing: {path}")
            with _parent(data, path) as origin, _parent(transaction, trash, create=True) as target:
                _rename_verified(origin, target)
            if durable:
                touched_directories.update({("data", path.parent), ("transaction", trash.parent)})
            continue

        if operation == "replace-tree":
            source = _relative_path(raw.get("source"), "replace-tree source")
            expected = str(raw.get("sha256", ""))
            trash = trash_root / "old"
            destination_state = _entry(data, path, follow=follow)
            source_state = _entry(transaction, source, follow=follow)
            if (
                destination_state is not None
                and _digest_matches(data, path, destination_state, expected)
                and source_state is None
            ):
                continue
            if destination_state is not None and transaction.stat(trash) is None:
                with _parent(data, path) as origin, _parent(transaction, trash, create=True) as target:
                    _rename_verified(origin, target)
                if durable:
                    touched_directories.add(("transaction", trash.parent))
            if (
                source_state is None
                or not stat.S_ISDIR(source_state.st_mode)
                or not _digest_matches(transaction, source, source_state, expected)
            ):
                raise TransactionError(f"replace-tree source digest mismatch: {path}")
            with _parent(transaction, source) as origin, _parent(data, path, create=True) as target:
                _rename_verified(origin, target)
            if durable:
                data.fsync_tree(path)
                touched_directories.add(("data", path.parent))
            continue

        raise FormatError(f"unsupported transaction operation: {operation!r}")
    if durable:
        for root, relative in sorted(touched_directories, key=lambda item: (item[0], item[1].as_posix())):
            _fsync_directory(roots[root], relative)


def _fsync_directory(root: JobDirectory, relative: PurePosixPath) -> None:
    """Synchronize one touched directory that is still a real directory."""

    if relative == PurePosixPath("."):
        os.fsync(root.fd)
        return
    try:
        with root.directory(relative) as directory:
            os.fsync(directory.fd)
    except (FileNotFoundError, JobDirectoryError) as exc:
        _LOGGER.debug("not synchronizing %s: %s", root.path.joinpath(*relative.parts), exc)
