"""Lock-free primitives of the transfer protocol: verified renames, no-replace links, trash and owners.

Every decision of the sealing transaction and the adoption chain is one
server-side atomic operation — the rename of a single source, a ``link(2)``
that never replaces, or the rename of a freshly built directory onto a name
that must not hold anything — and every step around it is repeatable. These
primitives add the one thing a network filesystem needs on top: after an
error, they decide what actually happened by looking at the outcome rather
than trusting the error code, because NFS retransmission can report
``ENOENT`` or ``EEXIST`` for an operation that took effect.

Liveness evidence (:func:`owner_gone`) only decides *when* an actor takes over
work somebody else started; correctness never depends on it.

The transaction modules call :func:`_hook` with a step name at every named
step of the protocol (``"S4.member"``, ``"V6.claim"``, ...). Tests set
:data:`_HOOK` to inject crashes and interleavings at exactly those points.
"""

import errno
import hashlib
import logging
import os
import re
import socket
import stat
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from ._util import fsync_directory, fsync_tree, read_json, timestamp_seconds, utc_now, write_json_atomic
from .errors import FormatError, WorkflowError

_LOGGER = logging.getLogger(__name__)

#: The test hook: called with the name of every protocol step the transaction
#: modules reach, so a test can raise (a crash) or run another actor (an
#: interleaving) at exactly that point. ``None`` outside tests.
_HOOK: Callable[[str], None] | None = None

#: How long a CLI owner (``httk job adopt``, ``httk job eject``) is presumed
#: alive when nothing on this host can prove that its process is gone.
CLI_OWNER_SECONDS = 24 * 3600
#: The largest name-encoded time: claim and seal times are signed 64-bit UTC nanoseconds.
MAXIMUM_NS = (1 << 63) - 1
#: The bound of the walk :func:`holds_job_payload` makes before it answers "yes" to be safe.
PAYLOAD_SCAN_LIMIT = 100_000

_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_NS_PATTERN = re.compile(r"0|[1-9][0-9]{0,18}")
_MANAGER_TOKEN = re.compile(r"m([0-9a-f]{32})")
_CLI_TOKEN = re.compile(r"c([0-9a-f]{8})(0|[1-9][0-9]{0,9})([0-9a-f]{8})")
_BOOT_ID_PATH = Path("/proc/sys/kernel/random/boot_id")
#: The boot hash used when the boot identity cannot be read. It never matches
#: as "this boot", so the immediate dead-process rule is never applied blind.
_UNKNOWN_BOOT = "00000000"
#: Marks the private temporary a :func:`link_new` call creates beside its target.
_LINK_TEMPORARY = ".link-"


def _hook(step: str) -> None:
    """Report reaching one named protocol step to the test hook, if one is installed.

    :param step: The step name, such as ``"S4.member"``.
    """

    hook = _HOOK
    if hook is not None:
        hook(step)


# ---------------------------------------------------------------------------
# Name-encoded times
# ---------------------------------------------------------------------------


def format_ns(value: int) -> str:
    """Return the canonical text of one name-encoded UTC nanosecond time.

    :param value: The time in integer UTC nanoseconds.
    :return: Its decimal text, without sign or leading zeros.
    :raises ValueError: If the value is not a nonnegative signed 64-bit integer.
    """

    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= MAXIMUM_NS:
        raise ValueError(f"a protocol time must be a nonnegative 64-bit integer of nanoseconds, not {value!r}")
    return str(value)


def parse_ns(text: str) -> int:
    """Parse one name-encoded UTC nanosecond time strictly.

    :param text: The decimal text, as :func:`format_ns` writes it.
    :return: The time in integer UTC nanoseconds.
    :raises httk.workflow.errors.FormatError: If the text is not canonical decimal or out of range.
    """

    if not isinstance(text, str) or not _NS_PATTERN.fullmatch(text):
        raise FormatError(f"invalid protocol time {text!r}: expected canonical decimal nanoseconds")
    value = int(text)
    if value > MAXIMUM_NS:
        raise FormatError(f"protocol time {text!r} exceeds the signed 64-bit range")
    return value


# ---------------------------------------------------------------------------
# Verified renames and no-replace links
# ---------------------------------------------------------------------------


def _probe(path: Path | str, dir_fd: int | None) -> os.stat_result | None:
    """Return ``lstat`` of one name, or ``None`` when it is absent; other errors propagate."""

    try:
        return os.lstat(path, dir_fd=dir_fd)
    except (FileNotFoundError, NotADirectoryError):
        return None


def rename_verified(
    src: Path | str,
    dst: Path | str,
    *,
    src_dir_fd: int | None = None,
    dst_dir_fd: int | None = None,
) -> bool:
    """Rename one single-source entry and decide its outcome by identity, not by the error code.

    The source's ``(st_dev, st_ino)`` is recorded first (comparing both is
    valid inside one process). After any error other than ``EXDEV``, the
    destination holding that identity means the rename happened (an NFS
    retransmission reported an error for it); the source still present means
    it failed and the original error is raised; both absent means another
    actor moved the source first. Relative names resolve against the optional
    directory descriptors, as for :func:`os.rename`.

    :param src: The single source to move.
    :param dst: The destination name.
    :param src_dir_fd: Resolve a relative *src* against this directory descriptor.
    :param dst_dir_fd: Resolve a relative *dst* against this directory descriptor.
    :return: ``True`` when this call moved the source to *dst*, ``False`` when
        another actor had already moved it (the caller lost).
    :raises OSError: ``EXDEV`` always (never translated into a copy), and any
        other error after which the source is still present, or whose outcome
        cannot be observed.
    """

    before = _probe(src, src_dir_fd)
    if before is None:
        return False
    identity = (before.st_dev, before.st_ino)
    try:
        os.rename(src, dst, src_dir_fd=src_dir_fd, dst_dir_fd=dst_dir_fd)
    except OSError as exc:
        if exc.errno == errno.EXDEV:
            raise
        try:
            after = _probe(dst, dst_dir_fd)
            moved = after is not None and (after.st_dev, after.st_ino) == identity
            source_present = not moved and _probe(src, src_dir_fd) is not None
        except OSError as probe_error:
            raise exc from probe_error
        if moved:
            _LOGGER.debug("rename %s -> %s reported %s but took effect", src, dst, exc)
            return True
        if source_present:
            raise
        _LOGGER.debug("rename %s -> %s lost: another actor moved the source (%s)", src, dst, exc)
        return False
    return True


def is_link_temporary(name: str) -> bool:
    """Report whether a directory entry is a temporary :func:`link_new` left by a crash.

    :param name: One directory entry name.
    :return: Whether the name has the shape of a :func:`link_new` temporary.
    """

    return name.startswith(".") and _LINK_TEMPORARY in name


def _at(directory: Path | int, name: str) -> tuple[str, int | None]:
    """Return the ``(path, dir_fd)`` pair naming *name* in a directory given by path or descriptor."""

    if isinstance(directory, int):
        return name, directory
    return str(directory / name), None


def _read_back(directory: Path | int, name: str, limit: int) -> bytes | None:
    """Read one regular file back without following a symlink; ``None`` when it is absent or not a file."""

    path, dir_fd = _at(directory, name)
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=dir_fd)
    except (FileNotFoundError, NotADirectoryError):
        return None
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            return None
        raise
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            return None
        data = bytearray()
        while chunk := os.read(descriptor, limit + 1 - len(data)):
            data.extend(chunk)
            if len(data) > limit:
                break
        return bytes(data)
    finally:
        os.close(descriptor)


def _sync_directory(directory: Path | int) -> None:
    if isinstance(directory, int):
        os.fsync(directory)
    else:
        fsync_directory(directory)


def link_new(
    directory: Path | int,
    name: str,
    data: bytes,
    *,
    nonce: bool = False,
    mode: int = 0o644,
    durable: bool = True,
) -> bool:
    """Create one file with exactly *data* at a name that must not exist yet, never replacing anything.

    The content goes to an fsynced temporary in the same directory first, which
    is then hard-linked to *name* — ``link(2)`` fails rather than replace — and
    unlinked. When the link reports ``EEXIST``, or any other error, the outcome
    is decided by looking at *name*: the temporary's own inode there means this
    call created it; identical bytes count as this call's result only when
    *data* carries a nonce (otherwise the same bytes may well be another
    actor's, and the caller compares). The directory is synchronized after a
    successful link.

    A crash between creating the temporary and unlinking it leaves a dot-named
    temporary that :func:`is_link_temporary` recognizes.

    :param directory: The directory, as a path (every operation is then by path)
        or as an open directory descriptor (every operation is then relative to it).
    :param name: The new file's name, one path component.
    :param data: The exact content.
    :param nonce: Whether *data* carries a value unique to this call, so identical
        bytes at *name* can only be this call's own earlier link.
    :param mode: The file mode of the new file.
    :param durable: Synchronize the temporary and the directory.
    :return: ``True`` when *name* now holds this call's file, ``False`` when it
        already held something else (or the same bytes without a nonce).
    :raises ValueError: If *name* is not one plain path component.
    :raises OSError: If the temporary cannot be written, or the link fails and
        *name* stays absent.
    """

    if not name or name in {".", ".."} or "/" in name or "\0" in name:
        raise ValueError(f"link_new needs one plain file name, not {name!r}")
    temporary = f".{name}{_LINK_TEMPORARY}{uuid.uuid4().hex}"
    temporary_path, dir_fd = _at(directory, temporary)
    target_path, _ = _at(directory, name)
    descriptor = os.open(
        temporary_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, mode, dir_fd=dir_fd
    )
    try:
        try:
            view = memoryview(data)
            while view:
                view = view[os.write(descriptor, view) :]
            os.fchmod(descriptor, mode)
            if durable:
                os.fsync(descriptor)
            identity = os.fstat(descriptor)
        finally:
            os.close(descriptor)
        _hook("link_new.written")
        try:
            os.link(temporary_path, target_path, src_dir_fd=dir_fd, dst_dir_fd=dir_fd, follow_symlinks=False)
        except OSError as exc:
            current = _probe(target_path, dir_fd)
            if current is not None and (current.st_dev, current.st_ino) == (identity.st_dev, identity.st_ino):
                _LOGGER.debug("link %s reported %s but took effect", target_path, exc)
            elif current is None:
                raise
            else:
                existing = _read_back(directory, name, len(data))
                if existing == data and nonce:
                    _LOGGER.debug("link %s reported %s; it holds this call's nonce content", target_path, exc)
                else:
                    return False
    finally:
        try:
            os.unlink(temporary_path, dir_fd=dir_fd)
        except FileNotFoundError:
            pass
    if durable:
        _sync_directory(directory)
    return True


# ---------------------------------------------------------------------------
# Removal: trash and quarantine
# ---------------------------------------------------------------------------


def _open_for_removal(parent_fd: int, name: str) -> int | None:
    """Unlink a non-directory, or open a directory for emptying; ``None`` when nothing is left to descend."""

    information = _probe(name, parent_fd)
    if information is None:
        return None
    if stat.S_ISDIR(information.st_mode):
        try:
            descriptor = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
        except (FileNotFoundError, NotADirectoryError):
            return None
        except OSError as exc:
            if exc.errno != errno.ELOOP:
                raise
        else:
            # A read-only directory (a copied runner tree) must become
            # writable before its entries can be unlinked; the descriptor is
            # the directory itself, never a symlink swapped in by name.
            if stat.S_IMODE(os.fstat(descriptor).st_mode) & 0o700 != 0o700:
                os.fchmod(descriptor, 0o700)
            return descriptor
    try:
        os.unlink(name, dir_fd=parent_fd)
    except FileNotFoundError:
        pass
    return None


def remove_tree(path: Path) -> None:
    """Remove a file or directory tree, never following a symlink and tolerating concurrent removers.

    Every step goes through directory descriptors opened ``O_NOFOLLOW``, an
    entry that vanished meanwhile is skipped (``ENOENT`` is someone else's
    removal), and the walk is iterative, so no nesting depth exhausts it.

    :param path: The entry to remove; an absent entry is not an error.
    :raises OSError: If an entry cannot be removed for any reason but its absence.
    """

    try:
        root_parent = os.open(path.parent, _DIRECTORY_FLAGS & ~os.O_NOFOLLOW)
    except FileNotFoundError:
        return
    # Each frame: the parent descriptor, the directory's name in it, the
    # directory's own descriptor, and the children still to remove.
    stack: list[tuple[int, str, int, list[str]]] = []
    try:
        descriptor = _open_for_removal(root_parent, path.name)
        if descriptor is None:
            return
        stack.append((root_parent, path.name, descriptor, os.listdir(descriptor)))
        while stack:
            parent_fd, name, descriptor, remaining = stack[-1]
            if remaining:
                child = remaining.pop()
                child_fd = _open_for_removal(descriptor, child)
                if child_fd is not None:
                    try:
                        children = os.listdir(child_fd)
                    except BaseException:
                        os.close(child_fd)
                        raise
                    stack.append((descriptor, child, child_fd, children))
                continue
            stack.pop()
            os.close(descriptor)
            try:
                os.rmdir(name, dir_fd=parent_fd)
            except FileNotFoundError:
                pass
    finally:
        for _, _, descriptor, _ in stack:
            os.close(descriptor)
        os.close(root_parent)


def holds_job_payload(path: Path) -> bool:
    """Report whether a tree may hold a job payload, answering ``True`` whenever unsure.

    A job payload is recognized by its ``job.json``, at any depth. The walk
    never follows a symlink, and a tree too large to walk within
    :data:`PAYLOAD_SCAN_LIMIT` entries, or one that cannot be read, is
    reported as holding one: :func:`trash` then quarantines rather than removes it.

    :param path: The tree to inspect.
    :return: Whether the tree holds, or may hold, a ``job.json``.
    """

    try:
        if not stat.S_ISDIR(os.lstat(path).st_mode):
            return path.name == "job.json"
    except FileNotFoundError:
        return False
    except OSError:
        return True
    pending = [path]
    visited = 0
    try:
        while pending:
            directory = pending.pop()
            with os.scandir(directory) as entries:
                for entry in entries:
                    visited += 1
                    if visited > PAYLOAD_SCAN_LIMIT or entry.name == "job.json":
                        return True
                    if entry.is_dir(follow_symlinks=False):
                        pending.append(Path(entry.path))
    except FileNotFoundError:
        # Something vanished mid-walk: unsure, unless the whole tree is gone.
        return os.path.lexists(path)
    except OSError:
        return True
    return False


def trash(path: Path, *, control: Path, holds_payload: Callable[[Path], bool] | None) -> bool:
    """Discard a protocol tree another actor may also be discarding, never destroying a job payload.

    The tree is first renamed to a unique ``<control>/tmp/trash.<rand>`` name,
    so it is private before anything is removed; ``ENOENT`` at that rename (or
    an outcome showing another actor moved it) means someone else is already
    discarding it. *holds_payload* then inspects the private copy: a payload
    found there is moved to the canonical quarantine
    (``<control>/quarantine/<id>/entry`` with a ``report.json``, the layout of
    :meth:`~httk.workflow.workspace.Workspace.quarantine`) and reported as an
    error event, never removed. Otherwise the copy is removed.

    The workspace control directory is a parameter rather than derived from
    *path*, because the trashed trees live in several places below it
    (``tmp/``, ``transfers/``) and the caller always knows its workspace.

    :param path: The tree to discard.
    :param control: The workspace control directory (``.httk-workspace``).
    :param holds_payload: Decide whether the trashed tree holds a job payload;
        ``None`` disables the check (the caller has proven it holds none).
    :return: ``True`` when this call discarded (or quarantined) the tree,
        ``False`` when another actor had already taken it.
    :raises OSError: ``EXDEV`` if *path* is not on the control directory's
        filesystem, or any error that leaves the outcome unobserved.
    """

    trashed = control / "tmp" / f"trash.{uuid.uuid4().hex}"
    if not rename_verified(path, trashed):
        _LOGGER.debug("trash: %s was already taken by another actor", path)
        return False
    _hook("trash.renamed")
    if holds_payload is not None and holds_payload(trashed):
        identifier = f"{int(time.time())}-{uuid.uuid4()}"
        destination = control / "quarantine" / identifier
        destination.mkdir(parents=True, exist_ok=True)
        entry = destination / "entry"
        if not rename_verified(trashed, entry):
            _LOGGER.error(
                "trash: %s held a job payload and vanished before it could be quarantined",
                path,
                extra={"event": "trash_quarantine_lost", "entry": str(path), "trash": str(trashed)},
            )
            return True
        write_json_atomic(
            destination / "report.json",
            {
                "original_path": str(path),
                "reason": "a discarded protocol tree held a job payload",
                "quarantined_at": utc_now(),
            },
            durable=True,
        )
        _LOGGER.error(
            "trash: %s held a job payload; quarantined it as %s instead of removing it",
            path,
            destination,
            extra={"event": "trash_quarantined", "entry": str(path), "quarantine": str(destination)},
        )
        return True
    remove_tree(trashed)
    return True


# ---------------------------------------------------------------------------
# Birth of a transaction directory
# ---------------------------------------------------------------------------


def birth(
    tmp_dir: Path,
    final_name: str,
    build: Callable[[Path], None],
    *,
    durable: bool = True,
    parent: Path | None = None,
) -> bool:
    """Build a directory privately and publish it complete under a name that must not hold anything.

    *build* fills a fresh ``<tmp_dir>/birth.<rand>/``, which is synchronized
    and then renamed onto ``<parent>/<final_name>`` (``parent`` defaults to
    *tmp_dir*). A directory rename fails onto a non-empty directory or a
    non-directory, so an existing transaction directory is never replaced (an
    *empty* directory would be replaced; *build* always creates the skeleton,
    and transaction names are fresh). On failure the private directory is
    removed again.

    :param tmp_dir: The workspace ``tmp/`` directory, where the directory is built.
    :param final_name: The published name, one path component (``"eject.<T>"``).
    :param build: Create the directory's initial content below the given path.
    :param durable: Synchronize the built tree and the published name's directory.
    :param parent: Publish into this directory instead of *tmp_dir* (the workspace
        root, for ``exchange/``); it must be on the filesystem of *tmp_dir*.
    :return: ``True`` when this call published the directory, ``False`` when the
        name was already taken.
    :raises ValueError: If *final_name* is not one plain path component.
    :raises OSError: If building or publishing fails for any other reason
        (``EXDEV`` when *parent* is on another filesystem).
    """

    if not final_name or final_name in {".", ".."} or "/" in final_name or "\0" in final_name:
        raise ValueError(f"birth needs one plain directory name, not {final_name!r}")
    staging = tmp_dir / f"birth.{uuid.uuid4().hex}"
    os.mkdir(staging, 0o755)
    try:
        build(staging)
        if durable:
            fsync_tree(staging)
        _hook("birth.built")
        try:
            born = rename_verified(staging, (tmp_dir if parent is None else parent) / final_name)
        except OSError as exc:
            if exc.errno not in {errno.ENOTEMPTY, errno.EEXIST, errno.ENOTDIR, errno.EISDIR}:
                raise
            _LOGGER.debug("birth of %s refused: the name is taken (%s)", final_name, exc)
            born = False
    except BaseException:
        remove_tree(staging)
        raise
    if not born:
        remove_tree(staging)
        return False
    if durable:
        fsync_directory(tmp_dir if parent is None else parent)
    return True


# ---------------------------------------------------------------------------
# Owners
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OwnerToken:
    """One parsed owner token (plan decision 15).

    :param kind: ``"manager"`` or ``"cli"``.
    :param manager_id: The manager's canonical UUID, for a manager owner.
    :param host: The 8-hex host hash, for a CLI owner.
    :param pid: The process id, for a CLI owner.
    :param boot: The 8-hex boot hash, for a CLI owner.
    """

    kind: str
    manager_id: str | None = None
    host: str | None = None
    pid: int | None = None
    boot: str | None = None


def _short_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", "surrogateescape")).hexdigest()[:8]


def _host_hash() -> str:
    return _short_hash(socket.gethostname())


def _boot_hash() -> str:
    try:
        boot_id = _BOOT_ID_PATH.read_text(encoding="ascii").strip()
    except (OSError, UnicodeError):
        return _UNKNOWN_BOOT
    return _short_hash(boot_id) if boot_id else _UNKNOWN_BOOT


def owner_token(manager_id: str | None = None) -> str:
    """Return the owner token of a manager, or of this CLI process.

    A manager's token is ``m<manager-id-hex>``; a CLI process's is
    ``c<host-hash8><pid><boot-hash8>`` (the hostname, the process id and
    ``/proc/sys/kernel/random/boot_id``). Both are ``[0-9a-z]`` only, so they
    can be part of a file name.

    :param manager_id: The manager's canonical UUID, or ``None`` for this CLI process.
    :return: The owner token.
    :raises ValueError: If *manager_id* is not a UUID.
    """

    if manager_id is not None:
        return f"m{uuid.UUID(manager_id).hex}"
    return f"c{_host_hash()}{os.getpid()}{_boot_hash()}"


def parse_owner_token(token: str) -> OwnerToken:
    """Parse an owner token strictly.

    :param token: A token :func:`owner_token` produced.
    :return: The parsed token.
    :raises httk.workflow.errors.FormatError: If the token has neither shape.
    """

    if isinstance(token, str):
        manager = _MANAGER_TOKEN.fullmatch(token)
        if manager is not None:
            return OwnerToken("manager", manager_id=str(uuid.UUID(manager.group(1))))
        cli = _CLI_TOKEN.fullmatch(token)
        if cli is not None:
            return OwnerToken("cli", host=cli.group(1), pid=int(cli.group(2)), boot=cli.group(3))
    raise FormatError(f"invalid owner token {token!r}")


def _process_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OverflowError:
        return False
    return True


def owner_gone(control: Path, token: str, *, since: int, now: int, lease_seconds: float) -> bool:
    """Report whether the owner of some protocol work is evidently gone, so another actor may take over.

    A manager owner is gone when its directory below ``managers/`` is absent,
    or when its heartbeat is older than ``lease_seconds *
    DEFAULT_TAKEOVER_GRACE_FACTOR`` (an unreadable heartbeat is aged from
    *since* instead). A CLI owner is gone :data:`CLI_OWNER_SECONDS` after
    *since*, or at once when its token names this host and boot and its
    process is not alive. This only decides *when* to take over; the takeover
    itself is fenced by renames and is correct either way.

    :param control: The workspace control directory.
    :param token: The owner token recorded with the work.
    :param since: When the owner started the work, in integer UTC nanoseconds (name-encoded).
    :param now: The current time, in integer UTC nanoseconds.
    :param lease_seconds: The workspace manager lease (``WorkspacePolicy.lease_seconds``).
    :return: Whether the owner is evidently gone.
    :raises httk.workflow.errors.FormatError: If *token* is not an owner token.
    """

    parsed = parse_owner_token(token)
    if parsed.kind == "manager":
        from .manager import DEFAULT_TAKEOVER_GRACE_FACTOR

        grace = lease_seconds * DEFAULT_TAKEOVER_GRACE_FACTOR
        manager_dir = control / "managers" / str(parsed.manager_id)
        if _probe(manager_dir, None) is None:
            return True
        try:
            heartbeat = read_json(manager_dir / "heartbeat.json")
            updated = timestamp_seconds(str(heartbeat["updated_at"]))
        except (WorkflowError, KeyError, ValueError):
            return now - since > grace * 1e9
        return now / 1e9 - updated > grace
    boot = _boot_hash()
    same_boot = boot != _UNKNOWN_BOOT and parsed.boot == boot
    if same_boot and parsed.host == _host_hash() and not _process_alive(int(parsed.pid or 0)):
        return True
    return now - since > CLI_OWNER_SECONDS * 10**9
