"""Read the daemon's Bubblewrap execution identity packet exactly once."""

import json
import os
import re
from pathlib import Path

from ._death import ProcessIdentity

_MAX_PACKET_BYTES = 4096
_HOSTNAME = re.compile(r"[A-Za-z0-9_.-]{1,255}\Z")
_BOOT_ID = re.compile(r"[0-9a-fA-F-]{1,64}\Z")


def _pid_one_start_ticks() -> int:
    try:
        with Path("/proc/1/stat").open("rb") as stream:
            data = stream.read(_MAX_PACKET_BYTES + 1)
        if not data or len(data) > _MAX_PACKET_BYTES:
            raise ValueError
        close = data.rindex(b")")
        pid = int(data[: data.index(b" ")])
        ticks = int(data[close + 2 :].split()[19])
    except (OSError, ValueError, IndexError) as exc:
        raise ValueError("namespace PID 1 stat record is malformed or unavailable") from exc
    if pid != 1 or ticks < 0:
        raise ValueError("namespace PID 1 stat record has an invalid identity")
    return ticks


def read_execution_scope(scope_fd: int, host_identity: str) -> ProcessIdentity:
    """Read Bubblewrap's child PID and combine it with host and namespace identity.

    :param scope_fd: The inherited read-only descriptor written by Bubblewrap ``--info-fd``.
    :param host_identity: Compact JSON containing the host hostname and boot ID.
    :return: The daemon execution scope as a host process identity.
    :raises ValueError: If the packet, host identity or namespace PID 1 stat is malformed.
    """

    try:
        try:
            packet = os.pread(scope_fd, _MAX_PACKET_BYTES + 1, 0)
            if not packet or len(packet) > _MAX_PACKET_BYTES:
                raise ValueError("daemon execution scope packet is empty or oversized")
            scope = json.loads(packet)
            host = json.loads(host_identity)
            if type(scope) is not dict or type(host) is not dict or set(host) != {"hostname", "boot_id"}:
                raise ValueError("daemon execution scope or host identity must be a JSON object")
            child_pid = scope.get("child-pid")
            hostname, boot_id = host["hostname"], host["boot_id"]
            if type(child_pid) is not int or child_pid <= 1:
                raise ValueError("Bubblewrap child-pid must be a host PID greater than one")
            if type(hostname) is not str or _HOSTNAME.fullmatch(hostname) is None:
                raise ValueError("host identity hostname is malformed")
            if type(boot_id) is not str or _BOOT_ID.fullmatch(boot_id) is None:
                raise ValueError("host identity boot_id is malformed")
            return ProcessIdentity(hostname, boot_id, child_pid, _pid_one_start_ticks())
        except (OSError, TypeError, UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
            raise ValueError("daemon execution scope or host identity is malformed") from exc
    finally:
        try:
            os.close(scope_fd)
        except OSError:
            pass
