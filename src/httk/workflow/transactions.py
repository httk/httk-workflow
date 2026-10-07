"""Replay of optional transactional-data outcomes.

A replay runs on descriptors. The manager pins the data directory, and opens
the transaction directory of its commit draft afresh, by name, for every
access, so a takeover that renames the draft fences the replay; every component
of every operation path below them is opened ``O_DIRECTORY|O_NOFOLLOW``, so a
symlink or special file planted anywhere on an operation's source or
destination path fails the replay instead of redirecting a rename, and nothing
outside those two directories is touched. The public :func:`replay_transaction` is the
runner's replay into its own workdir and follows symlinks.

The manager's replay keeps every owner's trash apart: the holder of committing
generation ``g`` moves aside into ``trash/<op>.<g>/`` and creates data-side
directories there before renaming them into ``data/``. Before its first data
step it retires every predecessor's ``trash/<op>.<k>/`` (``rmdir``), and a
directory removed that way accepts no new entry even through a descriptor a
frozen predecessor still holds, so that predecessor can never change ``data/``
again.
"""

import contextlib
import itertools
import logging
import os
import stat
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

from . import _txn
from ._jobdir import CONTROL_DOCUMENT_LIMIT, JobDirectory, JobDirectoryError
from ._util import retry_delay
from .errors import FormatError, TransactionError, WorkflowError, WorkspaceUnavailableError
from .models import validate_label

_LOGGER = logging.getLogger(__name__)


class _DisplacedDataError(TransactionError):
    """A replay moved data it had not observed into its trash: another replayer's result was displaced.

    A second line of defence: each manager owner sets aside into its own
    trash, which a successor retires before replaying, so only a rename into
    that trash in the syscall gap before it can meet this. Unlike every other
    fenced error it means data is out of place, so it is reported loudly even
    when fenced.
    """


class _CommitDeferredError(WorkflowError):
    """A predecessor's trash directory cannot be retired yet, so this commit waits for the next tick.

    While that directory exists, a frozen predecessor could still rename into
    it, so the replay must not touch data; waiting decides only when it does.
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


def _delete(parent: JobDirectory, name: str) -> bool:
    """Delete one entry and everything below it in a few passes; return whether it is gone.

    A pass that meets an entry another deleter removed first, or a newcomer
    that made a directory non-empty again, is simply repeated.
    """

    for _ in range(3):
        try:
            parent.remove_tree(name)
        except OSError as exc:
            _LOGGER.debug("cannot delete %s yet: %s", parent.path / name, exc)
        if parent.stat(name) is None:
            return True
    return False


@dataclass
class _Owner:
    """The manager's replay as the holder of committing generation *generation*.

    Its trash for operation ``<op>`` is ``trash/<op>.<generation>``; every
    generation from *base_generation* below its own may have held the same
    commit before (a takeover or a request bumps the generation, and a
    processing run names its trash after the generation it holds), so those
    are its predecessors. Names are always built, never parsed from a listing:
    labels may contain dots, and only the integer after the last one is the
    generation.
    """

    transaction: Callable[[], contextlib.AbstractContextManager[JobDirectory]]
    generation: int
    base_generation: int
    durable: bool
    #: Operations with a trash directory of some generation: some owner saw them applicable.
    evidence: set[str] = field(default_factory=set)
    #: ``replace-tree`` operations whose old tree a predecessor had already set aside.
    set_aside: set[str] = field(default_factory=set)

    def own(self, operation_id: str) -> PurePosixPath:
        return PurePosixPath("trash", f"{operation_id}.{self.generation}")

    def create(self, operation_id: str) -> None:
        """Create this owner's trash directory for one operation below a fresh fence (fence 1)."""

        name = self.own(operation_id).name
        with self.transaction() as opened, opened.directory("trash") as trash:
            _txn._hook("replay.trash_create")
            created = True
            try:
                os.mkdir(name, dir_fd=trash.fd)
            except FileExistsError:
                created = False
            information = trash.stat(name)
            if information is not None and not stat.S_ISDIR(information.st_mode):
                raise JobDirectoryError(f"{trash.path / name} is a symlink or not a directory")
            if created and self.durable:
                # The directory is the evidence that this operation started; it
                # must be on storage before a predecessor's directory is
                # removed and before the data step it vouches for.
                os.fsync(trash.fd)
        _txn._hook("replay.trash_created")
        self.evidence.add(operation_id)

    @contextlib.contextmanager
    def trash(self, operation_id: str) -> Iterator[JobDirectory]:
        """Yield this owner's trash directory of one operation, opened after a second fence without creating it.

        A predecessor that wakes after a successor retired its directory either
        creates one empty directory and stops at the second fence, or fails to
        open the directory it never reached.
        """

        self.create(operation_id)
        with self.transaction() as opened:
            _txn._hook("replay.trash_open")
            with opened.directory(self.own(operation_id)) as own:
                yield own

    def prepare(self, operations: Sequence[Mapping[str, object]], data: JobDirectory) -> None:
        """Retire every predecessor's trash before the replay changes data, carrying its evidence forward.

        For each operation in order: decide whether it applies, probe every
        predecessor directory by fixed name, create this owner's directory when
        the operation applies or any directory of it exists (before retiring
        one, so the evidence that it started is never lost), then retire the
        predecessors' directories. A predecessor directory that cannot be
        removed defers the commit: replaying while it exists would let a late
        rename of its owner land there.

        :param operations: The validated manifest operations.
        :param data: The pinned data directory.
        :raises _CommitDeferredError: If a predecessor directory cannot be retired now.
        """

        with self.transaction() as opened:
            opened.directory("trash", create=True).close()
        for raw in operations:
            operation_id = validate_label(raw.get("id"), "transaction operation id")
            operation = raw.get("op")
            path = _relative_path(raw.get("path"), "transaction operation path")
            found: list[str] = []
            with self.transaction() as opened:
                for generation in range(self.base_generation, self.generation + 1):
                    name = PurePosixPath("trash", f"{operation_id}.{generation}")
                    information = opened.stat(name)
                    if information is None:
                        continue
                    if not stat.S_ISDIR(information.st_mode):
                        raise JobDirectoryError(f"{opened.path.joinpath(*name.parts)} is a symlink or not a directory")
                    self.evidence.add(operation_id)
                    if opened.stat(name / "old") is not None:
                        self.set_aside.add(operation_id)
                    if generation != self.generation:
                        found.append(name.name)
                if operation == "remove":
                    applicable = _entry(data, path, follow=False) is not None
                elif operation == "make-dir":
                    applicable = _entry(data, path, follow=False) is None
                elif operation in {"put-file", "put-tree", "replace-tree"}:
                    source = _relative_path(raw.get("source"), f"{operation} source")
                    applicable = operation_id in self.set_aside or _entry(opened, source, follow=False) is not None
                else:
                    applicable = False
            if applicable or operation_id in self.evidence:
                self.create(operation_id)
            for predecessor in found:
                with self.transaction() as opened, opened.directory("trash") as trash:
                    if not _delete(trash, predecessor):
                        raise _CommitDeferredError(f"cannot retire the trash directory {trash.path / predecessor} yet")
                _txn._hook("replay.predecessor_retired")
        if self.durable:
            # trash/ itself, when this pass created it.
            with self.transaction() as opened:
                os.fsync(opened.fd)

    def discard(self, own: JobDirectory, name: str) -> None:
        """Delete what this owner moved into its trash; whatever resists is left for garbage collection."""

        # ponytail: own leftovers wait for gc transaction_trash; retry here if gc lags in practice
        if not _delete(own, name):
            _LOGGER.debug("leaving %s for garbage collection", own.path / name)
        _txn._hook("replay.deleted")

    def make_directories(self, data: JobDirectory, relative: PurePosixPath, operation_id: str) -> list[PurePosixPath]:
        """Create every missing level of *relative* in data, outermost first, each renamed in from this owner's trash.

        The data descriptor never creates anything, so a frozen owner's late
        creation has a dead source once its trash is retired. After each
        rename the level must be the directory just created or an existing
        one; only then is the next level's parent opened.

        :param data: The pinned data directory.
        :param relative: The directory to create below *data*, with its parents.
        :param operation_id: The operation whose trash the directories come from.
        :return: The levels this call created.
        """

        created: list[PurePosixPath] = []
        for depth in range(1, len(relative.parts) + 1):
            level = PurePosixPath(*relative.parts[:depth])
            information = _entry(data, level, follow=False)
            if information is not None:
                if not stat.S_ISDIR(information.st_mode):
                    raise JobDirectoryError(f"{data.path.joinpath(*level.parts)} is not a directory")
                continue
            with self.trash(operation_id) as own, _parent(data, level) as target:
                fresh = _fresh_directory(own)
                made = own.stat(fresh)
                _txn._hook("replay.directory_made")
                error: OSError | None = None
                try:
                    os.rename(fresh, target.name, src_dir_fd=own.fd, dst_dir_fd=target.parent.fd)
                except OSError as exc:
                    error = exc
                landed = target.parent.stat(target.name)
                if landed is None:
                    raise WorkspaceUnavailableError(f"cannot create {target.shown}: {error}")
                if not stat.S_ISDIR(landed.st_mode):
                    raise JobDirectoryError(f"{target.shown} is a symlink or not a directory")
                if made is not None and landed.st_ino == made.st_ino:
                    created.append(level)
                else:
                    # It already existed: the creation happened before.
                    with contextlib.suppress(OSError):
                        os.rmdir(fresh, dir_fd=own.fd)
        return created


def _fresh_directory(directory: JobDirectory) -> str:
    """Create an empty directory ``new<n>`` with the first free *n* and return its name."""

    for index in itertools.count():
        name = f"new{index}"
        try:
            os.mkdir(name, dir_fd=directory.fd)
        except FileExistsError:
            continue
        return name
    raise AssertionError("unreachable")


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
    generation: int,
    base_generation: int,
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

    The replay acts as the holder of committing *generation*: its trash is
    ``trash/<op>.<generation>``, and before its first data step it retires the
    trash of every generation from *base_generation* below its own
    (:class:`_Owner`), or raises :class:`_CommitDeferredError` when one cannot
    be retired yet.
    """

    from ._manager_commit import open_draft

    @contextlib.contextmanager
    def transaction() -> Iterator[JobDirectory]:
        with open_draft(control, draft) as root, root.directory("transaction") as opened:
            yield opened

    owner = _Owner(transaction, generation, base_generation, durable)
    return _replay_opened(
        transaction, data, expected_generation=expected_generation, durable=durable, follow=False, owner=owner
    )


def _replay_opened(
    transaction: Callable[[], contextlib.AbstractContextManager[JobDirectory]],
    data: JobDirectory,
    *,
    expected_generation: int,
    durable: bool,
    follow: bool,
    owner: _Owner | None = None,
) -> bool:
    """Replay one transaction whose directory *transaction* opens afresh for every access."""

    with transaction() as opened:
        manifest = opened.read_json("manifest.json", CONTROL_DOCUMENT_LIMIT)
    operations = _validated_operations(manifest, expected_generation)
    _replay(transaction, data, operations, durable=durable, follow=follow, owner=owner)
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
    owner: _Owner | None = None,
) -> None:
    # *transaction* opens the transaction directory afresh: every access below
    # is one short operation, and no descriptor inside it is held from one
    # operation to the next, so a commit draft renamed by a takeover fences this
    # replay at its next access.
    #
    # The manager's replay (*owner* given) first retires every predecessor's
    # trash, so a fenced predecessor can no longer move or create anything in
    # data; its own trash is trash/<op>.<g>, and data-side directories come
    # from there. A fenced predecessor's put-* rename can still be in flight
    # until then, so an absent source never concludes corruption by itself:
    # the destination is re-observed first, and only a positively inconsistent
    # state raises. The runner's replay (*owner* None) keeps trash/<op>.
    #
    # Directories whose entries this replay changed, as (root, relative path)
    # pairs. A durable replay synchronizes each of them once at the end rather
    # than per operation, so a transaction touching many files under one
    # directory pays one directory fsync, not one per file.
    touched_directories: set[tuple[str, PurePosixPath]] = set()

    def create_directories(relative: PurePosixPath, operation_id: str) -> None:
        # The manager never creates through the data descriptor; the runner's
        # walks below create missing parents themselves.
        if owner is None or relative == PurePosixPath("."):
            return
        created = owner.make_directories(data, relative, operation_id)
        if durable:
            touched_directories.update(("data", level.parent) for level in created)

    @contextlib.contextmanager
    def trash_entry(operation_id: str, name: str) -> Iterator[_Location]:
        if owner is None:
            with (
                transaction() as opened,
                _parent(opened, PurePosixPath("trash", operation_id, name), create=True) as target,
            ):
                yield target
        else:
            with owner.trash(operation_id) as own:
                yield _Location(own, name)

    if owner is not None:
        owner.prepare(operations, data)
    for raw in operations:
        operation_id = validate_label(raw.get("id"), "transaction operation id")
        operation = raw.get("op")
        path = _relative_path(raw.get("path"), "transaction operation path")
        trash_root = PurePosixPath("trash", operation_id) if owner is None else owner.own(operation_id)

        if operation == "make-dir":
            existing = _entry(data, path, follow=follow)
            if existing is not None and not stat.S_ISDIR(existing.st_mode):
                raise TransactionError(f"make-dir destination is not a directory: {path}")
            if owner is None:
                data.directory(path, create=True).close()
            else:
                create_directories(path, operation_id)
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
            create_directories(path.parent, operation_id)
            with (
                transaction() as opened,
                _parent(opened, source) as origin,
                _parent(data, path, create=owner is None) as target,
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
            create_directories(path.parent, operation_id)
            with (
                transaction() as opened,
                _parent(opened, source) as origin,
                _parent(data, path, create=owner is None) as target,
            ):
                _rename_verified(origin, target)
            if durable:
                data.fsync_tree(path)
                touched_directories.add(("data", path.parent))
            continue

        if operation == "remove":
            if _entry(data, path, follow=follow) is None:
                # The rename into the trash is one step: once the target is
                # absent, a trash entry proves that the removal happened. The
                # manager deletes what it removed, so its evidence is a trash
                # directory of any generation, observed before any was retired.
                if owner is None:
                    with transaction() as opened:
                        removed = opened.stat(trash_root / "removed") is not None
                else:
                    removed = operation_id in owner.evidence
                if removed or bool(raw.get("missing_ok", False)):
                    continue
                raise TransactionError(f"remove target is missing: {path}")
            with _parent(data, path) as origin, trash_entry(operation_id, "removed") as target:
                _rename_verified(origin, target)
                if owner is not None:
                    _txn._hook("replay.moved")
                    owner.discard(target.parent, target.name)
            if durable:
                touched_directories.add(("data", path.parent))
                if owner is None:
                    touched_directories.add(("transaction", trash_root))
            continue

        if operation == "replace-tree":
            source = _relative_path(raw.get("source"), "replace-tree source")
            expected = str(raw.get("sha256", ""))
            destination_state = _entry(data, path, follow=follow)
            with transaction() as opened:
                source_state = _entry(opened, source, follow=follow)
                # The manager also remembers a predecessor's set-aside it
                # observed before retiring that predecessor's trash.
                set_aside = opened.stat(trash_root / "old") is not None or (
                    owner is not None and operation_id in owner.set_aside
                )
            if destination_state is not None and source_state is None and _matches(data, path, expected, follow=follow):
                continue
            if destination_state is not None and not set_aside:
                with _parent(data, path) as origin, trash_entry(operation_id, "old") as target:
                    # Checked again right before the rename, as a second line
                    # of defence behind the manager's per-owner trash.
                    # ponytail: stat-then-rename leaves a syscall gap, because
                    # the stdlib has no RENAME_NOREPLACE; a rename that lands on
                    # an empty set-aside directory is detected below, not prevented.
                    if not target.exists():
                        _rename_verified(origin, target)
                        moved = target.parent.stat(target.name)
                        # In the manager's replay an absent entry means a
                        # successor retired this owner's trash, and its next
                        # access to the draft is fenced.
                        if (moved is None and owner is None) or (
                            moved is not None and moved.st_ino != destination_state.st_ino
                        ):
                            raise _DisplacedDataError(
                                f"replace-tree moved {origin.shown}, which was not the tree it observed there, "
                                f"onto {target.shown}: another commit of this transaction ran meanwhile, "
                                f"and its {path} now lies in that trash"
                            )
                if durable and owner is None:
                    touched_directories.add(("transaction", trash_root))
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
            create_directories(path.parent, operation_id)
            with (
                transaction() as opened,
                _parent(opened, source) as origin,
                _parent(data, path, create=owner is None) as target,
            ):
                _rename_verified(origin, target)
            if owner is not None:
                with owner.trash(operation_id) as own:
                    owner.discard(own, "old")
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
