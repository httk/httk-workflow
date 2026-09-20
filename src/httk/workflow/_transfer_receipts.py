"""Compact replay protection for sequenced transfers.

All accesses run under the workspace protocol lock. The lock inode is permanent:
removing a lock file would allow two processes to lock different inodes. POSIX
flock support on the shared filesystem is required, just as for logging locks.
A receipt range contains no job identifiers, digests, or payload paths. Disjoint
ranges are necessary only while lower sequence numbers remain unreceived.
"""

import fcntl
import os
import socket
import uuid
from collections.abc import Callable, Mapping
from functools import wraps
from typing import Any

from ._util import fsync_directory, read_json, write_json_atomic
from .errors import FormatError
from .workspace import Workspace

# Never reconstruct a session from disk. A restart (including fork) must mint a
# new epoch, even if every allocator file was consistently restored in place.
# Retaining the last published stream in memory also detects rollback while the
# process stays alive. Entries scale with workspace peers, not transfers.
_session_streams: dict[tuple[object, ...], dict[str, Any]] = {}


def serialized[**P, T](function: Callable[P, T]) -> Callable[P, T]:
    """Serialize a protocol operation, releasing its lock on process death."""

    @wraps(function)
    def locked(*args: P.args, **kwargs: P.kwargs) -> T:
        workspace = args[0] if args else kwargs["workspace"]
        assert isinstance(workspace, Workspace)
        directory = workspace.control / "transfers" / "protocol"
        directory.mkdir(parents=True, exist_ok=True)
        if workspace.durable:
            fsync_directory(directory.parent)
            fsync_directory(workspace.control)
        with (directory / "lock").open("a+b") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            try:
                return function(*args, **kwargs)
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    return locked


def _read(workspace: Workspace, name: str) -> dict[str, Any]:
    path = workspace.control / "transfers" / "protocol" / f"{name}.json"
    return read_json(path) if path.exists() else {}


def _write(workspace: Workspace, name: str, value: Mapping[str, Any]) -> None:
    write_json_atomic(workspace.control / "transfers" / "protocol" / f"{name}.json", value, durable=workspace.durable)


def _origin(workspace: Workspace) -> list[object]:
    info = workspace.control.stat()
    return [socket.gethostname(), str(workspace.root), info.st_dev, info.st_ino]


def _publish_issued(workspace: Workspace, streams: dict[str, Any]) -> None:
    """Publish diagnostic/retry state; neither disk copy can authorize an epoch."""

    snapshot = {"origin": _origin(workspace), "streams": streams}
    _write(workspace, "issued-checkpoint", snapshot)
    _write(workspace, "issued", streams)


def reserve(workspace: Workspace, peer: str, job_id: str, transfer_id: str) -> tuple[str, int, str]:
    """Allocate only in this process's volatile stream, never a restored epoch.

    Reservations can be reused before fencing is attempted. Once fencing may
    have published, only its state/manifest can authorize reuse. A new process
    reissues unfenced reservations without adopting an old epoch.
    """

    streams = _read(workspace, "issued")
    checkpoint = _read(workspace, "issued-checkpoint")
    key = (os.getpid(), *_origin(workspace), peer)
    if checkpoint.get("origin") != _origin(workspace) or checkpoint.get("streams") != streams:
        # Never recover old counters into a new stream. Existing sealed bundles
        # keep their immutable epoch; only newly allocated transfers rotate.
        streams = {}
    stream = streams.get(peer)
    current = _session_streams.get(key)
    if current is None or stream is None or (stream.get("epoch") == current["epoch"] and stream != current):
        stream = streams[peer] = {"epoch": str(uuid.uuid4()), "last": 0, "pending": {}}
    else:
        # Another process may have published its own epoch since our last call.
        # Keep our volatile counter; never adopt theirs, or rotate on every
        # interleaved job. A same-epoch disk rollback above starts a new stream.
        if stream.get("epoch") != current["epoch"]:
            # A different process may also have sealed/completed a reservation
            # we still remember. Without its disk evidence it cannot be reused,
            # even if that job has since returned to this workspace.
            current["pending"].clear()
        stream = streams[peer] = current
    _session_streams[key] = stream
    pending = stream["pending"]
    reservation = next((value for value in pending.values() if value[0] == job_id), None)
    if reservation is None:
        stream["last"] += 1
        pending[transfer_id] = [job_id, stream["last"]]
        _publish_issued(workspace, streams)
        return transfer_id, int(stream["last"]), str(stream["epoch"])
    identifier = next(key for key, value in pending.items() if value is reservation)
    return str(identifier), int(reservation[1]), str(stream["epoch"])


def fencing(workspace: Workspace, peer: str, transfer_id: str) -> None:
    """Forget reusable memory before fencing could publish or raise.

    Another process can subsequently seal and finish this transfer. Even if the
    pending disk reservation is then restored, it must never be reused for a
    new detach of the returned job. The disk reservation stays until sealing;
    an interrupted fencing attempt may conservatively waste an epoch.
    """

    current = _session_streams.get((os.getpid(), *_origin(workspace), peer))
    if current is not None:
        current["pending"].pop(transfer_id, None)


def sealed(workspace: Workspace, peer: str, transfer_id: str) -> None:
    """Pop only this exact transfer's reservation, never a later one for its job."""

    streams = _read(workspace, "issued")
    checkpoint = _read(workspace, "issued-checkpoint")
    fencing(workspace, peer, transfer_id)
    if checkpoint.get("origin") != _origin(workspace) or checkpoint.get("streams") != streams:
        return  # reserve() will rotate; do not certify a stale allocator here.
    stream = streams.get(peer)
    if stream is not None and stream["pending"].pop(transfer_id, None) is not None:
        _publish_issued(workspace, streams)


def epoch_of(manifest: Mapping[str, Any]) -> str | None:
    epoch = manifest.get("transfer_epoch")
    if epoch is None:
        return None
    try:
        if str(uuid.UUID(epoch)) != epoch:
            raise ValueError
    except (ValueError, TypeError, AttributeError) as exc:
        raise FormatError("transfer_epoch must be a canonical UUID") from exc
    return str(epoch)


def _stream_key(manifest: Mapping[str, Any]) -> str:
    peer = str(manifest["source_workspace_id"])
    epoch = epoch_of(manifest)
    return peer if epoch is None else f"{peer}/{epoch}"


def sequence_of(manifest: Mapping[str, Any]) -> int | None:
    """Read the optional sequence; old bundles retain their individual receipts."""

    sequence = manifest.get("transfer_sequence")
    if sequence is not None and (type(sequence) is not int or sequence < 1):
        raise FormatError("transfer_sequence must be a positive integer")
    return sequence


def received(workspace: Workspace, manifest: Mapping[str, Any]) -> bool:
    sequence = sequence_of(manifest)
    if sequence is None:
        return False
    ranges = _read(workspace, "received").get(_stream_key(manifest), [])
    return any(low <= sequence <= high for low, high in ranges)


def remember(workspace: Workspace, manifest: Mapping[str, Any]) -> None:
    """Durably record import before pruning its individual recovery records."""

    sequence = sequence_of(manifest)
    if sequence is None:
        return
    streams = _read(workspace, "received")
    peer = _stream_key(manifest)
    ranges = sorted([*streams.get(peer, []), [sequence, sequence]])
    merged: list[list[int]] = []
    for low, high in ranges:
        if merged and low <= merged[-1][1] + 1:
            merged[-1][1] = max(high, merged[-1][1])
        else:
            merged.append([low, high])
    streams[peer] = merged
    _write(workspace, "received", streams)
