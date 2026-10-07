"""Replay of optional transactional-data outcomes.

A replay runs on descriptors. The manager pins the data directory, and opens
the transaction directory of its commit draft afresh, by name, for every
access, so a takeover that renames the draft fences the replay; every component
of every operation path below them is opened ``O_DIRECTORY|O_NOFOLLOW``, so a
symlink or special file planted anywhere on an operation's source or
destination path fails the replay instead of redirecting a rename, and nothing
outside those two directories is touched. The public :func:`replay_transaction` is the
runner's replay into its own workdir and follows symlinks.
"""

import contextlib
import logging
import os
import stat
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from ._jobdir import CONTROL_DOCUMENT_LIMIT, JobDirectory, JobDirectoryError
from ._util import retry_delay
from .errors import FormatError, TransactionError, WorkspaceUnavailableError
from .models import validate_label

_LOGGER = logging.getLogger(__name__)


class _DisplacedDataError(TransactionError):
    """A replay moved data it had not observed into its trash: another replayer's result was displaced.

    Only a commit fenced by a takeover can meet this (its one step in flight
    raced the successor), and unlike every other fenced error it means data is
    out of place, so it is reported loudly even then.
    """


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
    with JobDirectory.at(data_dir, follow_symlinks=True) as data:
        return _replay_opened(
            lambda: JobDirectory.at(transaction_dir, follow_symlinks=True),
            data,
            expected_generation=expected_generation,
            durable=durable,
            follow=True,
        )


def _replay_pinned(
    control: JobDirectory,
    draft: str,
    data: JobDirectory,
    *,
    expected_generation: int,
    durable: bool = False,
) -> bool:
    """Replay the transaction of one commit draft into a pinned data directory; the manager's entry point.

    The draft is never pinned: every access to its ``transaction`` first opens
    the draft root by *draft* name below *control*, without creating it, so a
    takeover that renamed the draft stops this replay at its next access with
    :class:`~httk.workflow._manager_commit.CommitFencedError`. Every component
    below the draft root and the data directory is opened without following
    links, and a symlink or special file on any source or destination path,
    including the final name, fails the replay with
    :class:`~httk.workflow._jobdir.JobDirectoryError` rather than being followed
    or replaced.
    """

    from ._manager_commit import open_draft

    @contextlib.contextmanager
    def transaction() -> Iterator[JobDirectory]:
        with open_draft(control, draft) as root, root.directory("transaction") as opened:
            yield opened

    return _replay_opened(transaction, data, expected_generation=expected_generation, durable=durable, follow=False)


def _replay_opened(
    transaction: Callable[[], contextlib.AbstractContextManager[JobDirectory]],
    data: JobDirectory,
    *,
    expected_generation: int,
    durable: bool,
    follow: bool,
) -> bool:
    """Replay one transaction whose directory *transaction* opens afresh for every access."""

    with transaction() as opened:
        manifest = opened.read_json("manifest.json", CONTROL_DOCUMENT_LIMIT)
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


def _matches(root: JobDirectory, relative: PurePosixPath, expected: str, *, follow: bool) -> bool | None:
    """Return whether an entry has the expected digest, or ``None`` when it is absent.

    An entry that vanishes while it is digested (another replayer moved it) is
    absent, never an error.
    """

    information = _entry(root, relative, follow=follow)
    if information is None:
        return None
    try:
        return _digest_matches(root, relative, information, expected)
    except FileNotFoundError:
        return None


def _replay(
    transaction: Callable[[], contextlib.AbstractContextManager[JobDirectory]],
    data: JobDirectory,
    operations: Sequence[Mapping[str, object]],
    *,
    durable: bool,
    follow: bool,
) -> None:
    # *transaction* opens the transaction directory afresh: every access below
    # is one short operation, and no descriptor inside it is held from one
    # operation to the next, so a commit draft renamed by a takeover fences this
    # replay at its next access.
    #
    # Two replayers can overlap by at most one operation of the fenced one, so
    # an absent source never concludes corruption by itself: the destination is
    # re-observed first, and only a positively inconsistent state raises.
    #
    # Directories whose entries this replay changed, as (root, relative path)
    # pairs. A durable replay synchronizes each of them once at the end rather
    # than per operation, so a transaction touching many files under one
    # directory pays one directory fsync, not one per file.
    touched_directories: set[tuple[str, PurePosixPath]] = set()
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
            installed = _matches(data, path, expected, follow=follow)
            with transaction() as opened:
                source_state = _entry(opened, source, follow=follow)
                usable = (
                    None
                    if source_state is None or not stat.S_ISREG(source_state.st_mode)
                    else _matches(opened, source, expected, follow=follow)
                )
            if installed and source_state is None:
                continue
            if not usable:
                # The source may have been moved in since the destination was
                # observed: only a destination that still does not match is
                # corruption.
                if source_state is None and _matches(data, path, expected, follow=follow):
                    continue
                raise TransactionError(f"put-file source digest mismatch: {path}")
            with (
                transaction() as opened,
                _parent(opened, source) as origin,
                _parent(data, path, create=True) as target,
            ):
                _rename_verified(origin, target, replace=True)
            if not _matches(data, path, expected, follow=follow):
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
            with transaction() as opened:
                source_state = _entry(opened, source, follow=follow)
                usable = (
                    None
                    if source_state is None or not stat.S_ISDIR(source_state.st_mode)
                    else _matches(opened, source, expected, follow=follow)
                )
            if destination_state is not None and source_state is None and _matches(data, path, expected, follow=follow):
                continue
            if destination_state is not None:
                raise TransactionError(f"put-tree destination already exists: {path}")
            if not usable:
                if source_state is None and _matches(data, path, expected, follow=follow):
                    continue
                raise TransactionError(f"put-tree source digest mismatch: {path}")
            with (
                transaction() as opened,
                _parent(opened, source) as origin,
                _parent(data, path, create=True) as target,
            ):
                _rename_verified(origin, target)
            if durable:
                data.fsync_tree(path)
                touched_directories.add(("data", path.parent))
            continue

        if operation == "remove":
            trash = trash_root / "removed"
            if _entry(data, path, follow=follow) is None:
                # The rename into the trash is one step: once the target is
                # absent, a trash entry proves that the removal happened.
                with transaction() as opened:
                    removed = opened.stat(trash) is not None
                if removed or bool(raw.get("missing_ok", False)):
                    continue
                raise TransactionError(f"remove target is missing: {path}")
            with (
                transaction() as opened,
                _parent(data, path) as origin,
                _parent(opened, trash, create=True) as target,
            ):
                _rename_verified(origin, target)
            if durable:
                touched_directories.update({("data", path.parent), ("transaction", trash.parent)})
            continue

        if operation == "replace-tree":
            source = _relative_path(raw.get("source"), "replace-tree source")
            expected = str(raw.get("sha256", ""))
            trash = trash_root / "old"
            destination_state = _entry(data, path, follow=follow)
            with transaction() as opened:
                source_state = _entry(opened, source, follow=follow)
                set_aside = opened.stat(trash) is not None
            if destination_state is not None and source_state is None and _matches(data, path, expected, follow=follow):
                continue
            if destination_state is not None and not set_aside:
                with (
                    transaction() as opened,
                    _parent(data, path) as origin,
                    _parent(opened, trash, create=True) as target,
                ):
                    # Checked again right before the rename: a takeover may have
                    # set the old tree aside and installed the new one since.
                    # ponytail: stat-then-rename leaves a syscall gap, because
                    # the stdlib has no RENAME_NOREPLACE; a rename that lands on
                    # an empty set-aside directory is detected below, not prevented.
                    if not target.exists():
                        _rename_verified(origin, target)
                        moved = opened.stat(trash)
                        if moved is None or moved.st_ino != destination_state.st_ino:
                            raise _DisplacedDataError(
                                f"replace-tree moved {origin.shown}, which was not the tree it observed there, "
                                f"onto {target.shown}: another commit of this transaction ran meanwhile, "
                                f"and its {path} now lies in that trash"
                            )
                if durable:
                    touched_directories.add(("transaction", trash.parent))
            with transaction() as opened:
                source_state = _entry(opened, source, follow=follow)
                usable = (
                    None
                    if source_state is None or not stat.S_ISDIR(source_state.st_mode)
                    else _matches(opened, source, expected, follow=follow)
                )
            if not usable:
                if source_state is None and _matches(data, path, expected, follow=follow):
                    continue
                raise TransactionError(f"replace-tree source digest mismatch: {path}")
            with (
                transaction() as opened,
                _parent(opened, source) as origin,
                _parent(data, path, create=True) as target,
            ):
                _rename_verified(origin, target)
            if durable:
                data.fsync_tree(path)
                touched_directories.add(("data", path.parent))
            continue

        raise FormatError(f"unsupported transaction operation: {operation!r}")
    if durable:
        for root, relative in sorted(touched_directories, key=lambda item: (item[0], item[1].as_posix())):
            if root == "data":
                _fsync_directory(data, relative)
                continue
            with transaction() as opened:
                _fsync_directory(opened, relative)


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
