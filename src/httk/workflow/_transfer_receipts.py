"""Compact replay protection for sequenced transfers.

All accesses run under the workspace protocol lock. The lock inode is permanent:
removing a lock file would allow two processes to lock different inodes. POSIX
flock support on the shared filesystem is required, just as for logging locks.
A receipt range contains no job identifiers, digests, or payload paths. Disjoint
ranges are necessary only while lower sequence numbers remain unreceived.
"""

import fcntl
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


def reserve(workspace: Workspace, peer: str, job_id: str, transfer_id: str) -> tuple[str, int]:
    """Reserve without gaps; a crash before fencing leaves a reusable reservation."""

    streams = _read(workspace, "issued")
    stream = streams.setdefault(peer, {"last": 0, "pending": {}})
    pending = stream["pending"]
    if job_id not in pending:
        stream["last"] += 1
        pending[job_id] = [transfer_id, stream["last"]]
        _write(workspace, "issued", streams)
    identifier, sequence = pending[job_id]
    return str(identifier), int(sequence)


def sealed(workspace: Workspace, peer: str, job_id: str) -> None:
    """Forget the reservation only after the source ledger is durable."""

    streams = _read(workspace, "issued")
    stream = streams.get(peer)
    if stream is not None and stream["pending"].pop(job_id, None) is not None:
        _write(workspace, "issued", streams)


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
    ranges = _read(workspace, "received").get(str(manifest["source_workspace_id"]), [])
    return any(low <= sequence <= high for low, high in ranges)


def remember(workspace: Workspace, manifest: Mapping[str, Any]) -> None:
    """Durably record import before pruning its individual recovery records."""

    sequence = sequence_of(manifest)
    if sequence is None:
        return
    streams = _read(workspace, "received")
    peer = str(manifest["source_workspace_id"])
    ranges = sorted([*streams.get(peer, []), [sequence, sequence]])
    merged: list[list[int]] = []
    for low, high in ranges:
        if merged and low <= merged[-1][1] + 1:
            merged[-1][1] = max(high, merged[-1][1])
        else:
            merged.append([low, high])
    streams[peer] = merged
    _write(workspace, "received", streams)
