"""Job-data transactions (note §7.2): job-side staging and commit, manager-side application.

A job stages files into ``attempts/<A>/txn/<seq>.tmp/``, a tree that mirrors
the payload root, and commits it by renaming it to ``attempts/<A>/txn/<seq>/``.
While the job is quiescent its manager discards uncommitted staging and
renames every committed entry into the payload, in ``seq`` order, continuing
where a crashed predecessor stopped: a source that is gone was applied. The
payload root is job-writable, so the manager walks staging with
:func:`httk.workflow._fs.walk_untrusted`, never follows a symlink and only
enters real directories; with the job's processes gone, plain-path
validate-then-act is race-free.
"""

import errno
import os
import re
import shutil
import stat
from pathlib import Path, PurePosixPath
from typing import Self

from httk.workflow import _fs
from httk.workflow._kernel import OwnedJob
from httk.workflow.errors import FormatError, WorkflowError

#: Payload entries a transaction may never name at its top level.
RESERVED = frozenset({"job.json", "state.json", "seal.json", "logs", "attempts"})

_COMMITTED = re.compile(r"[0-9]{6}")
_STAGING = re.compile(r"[0-9]{6}\.tmp")


class DataConflict(WorkflowError):
    """A committed transaction cannot be applied: the payload holds the wrong kind of entry.

    :param path: The conflicting path, relative to the payload root.
    :param reason: Why the entry cannot be applied.
    """

    def __init__(self, path: PurePosixPath, reason: str) -> None:
        super().__init__(f"{path}: {reason}")
        self.path = path
        self.reason = reason


def _lstat(path: Path) -> os.stat_result | None:
    try:
        return os.lstat(path)
    except (FileNotFoundError, NotADirectoryError):
        return None


def _real_subdirectories(directory: Path) -> list[str]:
    # Sorted names of the real (never symlinked) subdirectories; nothing when *directory* is not a real directory.
    info = _lstat(directory)
    if info is None or not stat.S_ISDIR(info.st_mode):
        return []
    with os.scandir(directory) as listing:
        return sorted(entry.name for entry in listing if entry.is_dir(follow_symlinks=False))


def _transaction_dirs(job: OwnedJob, pattern: re.Pattern[str]) -> list[Path]:
    # attempts/<A>/txn/<name> for every real directory on the way whose last name matches *pattern*.
    attempts = job.path / "attempts"
    return [
        attempts / attempt / "txn" / name
        for attempt in _real_subdirectories(attempts)
        for name in _real_subdirectories(attempts / attempt / "txn")
        if pattern.fullmatch(name)
    ]


def _sync_tree(root: Path) -> None:
    # Durable commit: the staged content must reach the disk before the rename that commits it.
    for directory, _subdirectories, files in os.walk(root):
        for path in (Path(directory), *(Path(directory, name) for name in files)):
            if path.is_symlink():
                continue
            descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)


class Transaction:
    """A job-side transaction staged in the job's own attempt directory (runner code).

    :param control_dir: The attempt directory ``attempts/<A>``.
    :param durable: Fsync the staged content and the commit rename.
    """

    def __init__(self, control_dir: Path, *, durable: bool) -> None:
        txn = control_dir / "txn"
        txn.mkdir(parents=True, exist_ok=True)
        while True:
            used = [int(name[:6]) for name in os.listdir(txn) if _COMMITTED.fullmatch(name) or _STAGING.fullmatch(name)]
            seq = max(used, default=0) + 1
            if seq > 999_999:
                # The manager only recognizes six-digit names; a seventh digit would never be applied.
                raise WorkflowError(f"{txn}: an attempt holds at most 999999 transactions")
            self._final = txn / f"{seq:06d}"
            self.path = txn / f"{self._final.name}.tmp"
            try:
                # Exclusive: two transactions of one attempt never share a seq.
                self.path.mkdir()
            except FileExistsError:
                continue
            break
        self._durable = durable
        self._committed = False

    @classmethod
    def resume(cls, control_dir: Path, seq: str, *, durable: bool) -> Self:
        """Reattach to the uncommitted transaction *seq* an earlier process of this attempt began.

        A Bash runner is many short-lived processes; the staging directory is what carries a
        transaction from one bridge call to the next.

        :param control_dir: The attempt directory ``attempts/<A>``.
        :param seq: The six-digit sequence, as :attr:`seq` reports it.
        :param durable: Fsync the staged content and the commit rename.
        :return: The transaction.
        :raises WorkflowError: When no such uncommitted transaction exists.
        """

        if not _COMMITTED.fullmatch(seq) or not (control_dir / "txn" / f"{seq}.tmp").is_dir():
            raise WorkflowError(f"{control_dir}: no uncommitted transaction {seq!r}")
        transaction = cls.__new__(cls)
        transaction._final = control_dir / "txn" / seq
        transaction.path = control_dir / "txn" / f"{seq}.tmp"
        transaction._durable = durable
        transaction._committed = False
        return transaction

    @property
    def seq(self) -> str:
        """The six-digit sequence of this transaction within its attempt."""

        return self._final.name

    def put(self, source: str | os.PathLike[str], destination: str | PurePosixPath) -> None:
        """Stage a copy of a file or a directory tree (symlinks inside it stay links).

        :param source: The file or directory to copy.
        :param destination: The relative POSIX path in the payload.
        :raises WorkflowError: After :meth:`commit`.
        :raises ValueError: For an absolute, empty, ``..``-containing or reserved destination.
        :raises IsADirectoryError: For a file staged where a directory is already staged.
        """

        if self._committed:
            raise WorkflowError(f"{self.path}: the transaction is committed")
        relative = PurePosixPath(destination)
        if relative.is_absolute() or not relative.parts or ".." in relative.parts or relative.parts[0] in RESERVED:
            raise ValueError(f"invalid transaction destination: {str(destination)!r}")
        target = self.path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if os.path.isdir(source):
            shutil.copytree(source, target, symlinks=True, dirs_exist_ok=True)
        elif target.is_dir():
            raise IsADirectoryError(errno.EISDIR, "a directory is staged there", str(target))
        else:
            shutil.copy2(source, target)

    def commit(self) -> None:
        """Commit the staged tree; the manager applies it at the next attempt boundary.

        :raises WorkflowError: When already committed.
        """

        if self._committed:
            raise WorkflowError(f"{self.path}: the transaction is committed")
        if self._durable:
            _sync_tree(self.path)
        _fs.move_owned(_fs.loc(self.path), _fs.loc(self._final), durable=self._durable)
        self._committed = True


def discard_uncommitted(job: OwnedJob) -> int:
    """Remove every uncommitted staging tree ``attempts/*/txn/*.tmp``.

    :param job: The quiescent owned job.
    :return: The number of staging trees removed.
    :raises WorkflowError: While an attempt of the job is live.
    """

    job.require_quiescent()
    staging = _transaction_dirs(job, _STAGING)
    for path in staging:
        job.discard_subtree(path.relative_to(job.path).as_posix())
    return len(staging)


def apply_transactions(job: OwnedJob) -> int:
    """Apply every committed transaction of the job to its payload, in ``seq`` order.

    :param job: The quiescent owned job.
    :return: The number of entries moved into the payload.
    :raises WorkflowError: While an attempt of the job is live.
    :raises FormatError: For staging that holds an unsafe entry or names a reserved entry (a protocol error).
    :raises DataConflict: When the payload holds the wrong kind of entry; the remaining staging stays in place.
    """

    job.require_quiescent()
    applied = 0
    for txn in _transaction_dirs(job, _COMMITTED):
        applied += _apply(job, txn)
        _fs.remove_empty_dir(_fs.loc(txn.parent))
    return applied


def _apply(job: OwnedJob, txn: Path) -> int:
    try:
        entries = _fs.walk_untrusted(txn)
    except _fs.UntrustedContentError as exc:
        raise FormatError(f"{txn}: {exc}") from exc
    for entry in entries:
        if entry.relative.parts[0] in RESERVED:
            raise FormatError(f"{txn}: {entry.relative} names a reserved payload entry")
    durable, applied = job.owner.workspace.durable, 0
    # Top-down order: every staged directory is decided before its children, so a child's parents in the
    # payload are real directories (merged) or the child left with its parent (moved whole).
    for entry in entries:
        source, target = _fs.loc(txn / entry.relative), _fs.loc(job.path / entry.relative)
        if not _fs.exists(source):
            # Applied by a crashed predecessor, or moved together with its parent directory.
            continue
        info = _lstat(job.path / entry.relative)
        is_dir = info is not None and stat.S_ISDIR(info.st_mode)
        if entry.kind == "dir":
            if is_dir:
                # Merge: the following entries recurse into it.
                continue
            if info is not None:
                raise DataConflict(entry.relative, "a directory is staged over a non-directory")
        elif is_dir:
            raise DataConflict(entry.relative, f"a {entry.kind} is staged over a directory")
        try:
            # A whole directory to an absent target (the fast path), or a file or symlink over a file or symlink.
            _fs.move_owned(source, target, durable=durable)
        except _fs.MoveFailed as exc:
            if exc.errno in (errno.EISDIR, errno.ENOTDIR):
                raise DataConflict(entry.relative, f"cannot be moved into place: {os.strerror(exc.errno)}") from exc
            raise
        applied += 1
    for entry in reversed(entries):
        if entry.kind == "dir":
            _fs.remove_empty_dir(_fs.loc(txn / entry.relative))
    _fs.remove_empty_dir(_fs.loc(txn))
    return applied
