"""Read the daemon's Bubblewrap execution identity packet exactly once."""

import json
import os
import re
from pathlib import Path
from typing import Any

from httk.workflow._death import ProcessIdentity

_MAX_PACKET_BYTES = 4096
_HOSTNAME = re.compile(r"[A-Za-z0-9_.-]{1,255}\Z")
_BOOT_ID = re.compile(r"[0-9a-fA-F-]{1,64}\Z")


def _json_descriptor(descriptor: int, name: str) -> dict[str, Any]:
    try:
        data = os.pread(descriptor, _MAX_PACKET_BYTES + 1, 0)
    except OSError as exc:
        raise ValueError(f"cannot read daemon {name} metadata descriptor") from exc
    if not data or len(data) > _MAX_PACKET_BYTES:
        raise ValueError(f"daemon {name} metadata is empty or oversized")
    try:
        value = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"daemon {name} metadata is not valid JSON") from exc
    if type(value) is not dict:
        raise ValueError(f"daemon {name} metadata must be a JSON object")
    return value


def _pid_one_start_ticks() -> int:
    try:
        with Path("/proc/1/stat").open("rb") as stream:
            data = stream.read(_MAX_PACKET_BYTES + 1)
    except OSError as exc:
        raise ValueError("cannot read namespace PID 1 identity from /proc/1/stat") from exc
    if not data or len(data) > _MAX_PACKET_BYTES:
        raise ValueError("namespace PID 1 stat record is empty or oversized")
    try:
        close = data.rindex(b")")
        fields = data[close + 2 :].split()
        ticks = int(fields[19])
        pid = int(data[: data.index(b" ")])
    except (ValueError, IndexError) as exc:
        raise ValueError("namespace PID 1 stat record is malformed") from exc
    if pid != 1 or ticks < 0:
        raise ValueError("namespace PID 1 stat record has an invalid identity")
    return ticks


def read_execution_scope(scope_fd: int, host_identity_fd: int) -> ProcessIdentity:
    """Return the complete host identity of this Bubblewrap child and close both metadata descriptors.

    The scope packet must come from Bubblewrap ``--info-fd`` and name its host child PID. That PID must
    be PID 1 in the private proc mount. The independent host packet pins hostname and boot identity.

    :param scope_fd: The inherited read-only descriptor for the memfd written by Bubblewrap ``--info-fd``.
    :param host_identity_fd: The inherited sealed read-only host identity memfd.
    :return: The daemon execution scope as a host process identity.
    :raises ValueError: If either packet or the namespace PID 1 identity is incomplete or malformed.
    """

    try:
        scope = _json_descriptor(scope_fd, "execution scope")
        host = _json_descriptor(host_identity_fd, "host identity")
        child_pid = scope.get("child-pid")
        hostname, boot_id = host.get("hostname"), host.get("boot_id")
        if type(child_pid) is not int or child_pid <= 1:
            raise ValueError("Bubblewrap child-pid must be a host PID greater than one")
        if type(hostname) is not str or _HOSTNAME.fullmatch(hostname) is None:
            raise ValueError("host identity hostname is malformed")
        if type(boot_id) is not str or _BOOT_ID.fullmatch(boot_id) is None:
            raise ValueError("host identity boot_id is malformed")
        return ProcessIdentity(hostname, boot_id, child_pid, _pid_one_start_ticks())
    finally:
        for descriptor in {scope_fd, host_identity_fd}:
            try:
                os.close(descriptor)
            except OSError:
                pass
