"""Compact replay protection for sequenced transfers.

All accesses run under the workspace protocol lock. The lock inode is permanent:
removing a lock file would allow two processes to lock different inodes. POSIX
flock support on the shared filesystem is required, just as for logging locks.
A receipt range contains no job identifiers, digests, or payload paths. Disjoint
ranges are necessary only while lower sequence numbers remain unreceived.
"""

import fcntl
import socket
import uuid
from collections.abc import Callable, Mapping
from functools import wraps
from typing import Any

from ._util import fsync_directory, read_json, write_json_atomic
from .errors import FormatError
from .workspace import Workspace


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
    """Write-ahead checkpoint prevents an interrupted two-file update looking like rollback.

    The checkpoint is authoritative. A stale/missing issued copy rotates epochs
    on the next reservation; an interrupted write may waste an epoch, never
    reissue a sequence in one. Both files are fixed-size per peer at rest.
    """

    snapshot = {"origin": _origin(workspace), "streams": streams}
    _write(workspace, "issued-checkpoint", snapshot)
    _write(workspace, "issued", streams)


def reserve(workspace: Workspace, peer: str, job_id: str, transfer_id: str) -> tuple[str, int, str]:
    """Reserve in a fresh epoch after allocator reset, rollback, or workspace copy."""

    streams = _read(workspace, "issued")
    checkpoint = _read(workspace, "issued-checkpoint")
    if checkpoint.get("origin") != _origin(workspace) or checkpoint.get("streams") != streams:
        # Never recover old counters into a new stream. Existing sealed bundles
        # keep their immutable epoch; only newly allocated transfers rotate.
        streams = {}
    stream = streams.setdefault(peer, {"epoch": str(uuid.uuid4()), "last": 0, "pending": {}})
    if "epoch" not in stream:
        stream = streams[peer] = {"epoch": str(uuid.uuid4()), "last": 0, "pending": {}}
    pending = stream["pending"]
    reservation = next((value for value in pending.values() if value[0] == job_id), None)
    if reservation is None:
        stream["last"] += 1
        pending[transfer_id] = [job_id, stream["last"]]
        _publish_issued(workspace, streams)
        return transfer_id, int(stream["last"]), str(stream["epoch"])
    identifier = next(key for key, value in pending.items() if value is reservation)
    return str(identifier), int(reservation[1]), str(stream["epoch"])


def sealed(workspace: Workspace, peer: str, transfer_id: str) -> None:
    """Pop only this exact transfer's reservation, never a later one for its job."""

    streams = _read(workspace, "issued")
    checkpoint = _read(workspace, "issued-checkpoint")
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
