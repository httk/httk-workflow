"""Protected active-snapshot pointer helpers for the workspace daemon."""

import hashlib
import json
import os
import re
import stat
from pathlib import Path

from ._daemon_policy import Policy, _open_nofollow, policy_document

_FORMAT = "httk-workspace-daemon-activation"
_FORMAT_VERSION = 1
_MAX_BYTES = 4096
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")


def _canonical_policy(policy: Policy) -> bytes:
    return json.dumps(policy_document(policy), sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def activation_document(snapshot: Path, policy: Policy) -> dict[str, object]:
    """Return the protected pointer document for one runtime snapshot.

    :param snapshot: Absolute immutable runtime policy path.
    :param policy: Validated policy represented by the snapshot.
    :return: JSON-compatible activation object.
    :raises ValueError: If the snapshot path is invalid.
    """

    if not isinstance(snapshot, Path) or not snapshot.is_absolute() or ".." in snapshot.parts or "\0" in str(snapshot):
        raise ValueError("snapshot must be an absolute Path without '..' or NUL")
    return {
        "format": _FORMAT,
        "format_version": _FORMAT_VERSION,
        "snapshot": str(snapshot),
        "digest": hashlib.sha256(_canonical_policy(policy)).hexdigest(),
    }


def _read_pointer(state: Path) -> bytes:
    if not isinstance(state, Path) or not state.is_absolute() or ".." in state.parts or "\0" in str(state):
        raise ValueError("state must be an absolute Path without '..' or NUL")
    descriptor = _open_nofollow(state / "active.json")
    try:
        information = os.fstat(descriptor)
        if not stat.S_ISREG(information.st_mode):
            raise ValueError("active snapshot pointer must be a regular file")
        if information.st_mode & 0o002:
            raise ValueError("active snapshot pointer must not be world-writable")
        if information.st_size > _MAX_BYTES:
            raise ValueError("active snapshot pointer is too large")
        data = bytearray()
        while len(data) <= _MAX_BYTES:
            chunk = os.read(descriptor, _MAX_BYTES + 1 - len(data))
            if not chunk:
                return bytes(data)
            data.extend(chunk)
        raise ValueError("active snapshot pointer is too large")
    finally:
        os.close(descriptor)


def read_active_snapshot(state: Path) -> tuple[Path, str]:
    """Read and validate the protected active-snapshot pointer.

    :param state: Protected local daemon state directory.
    :return: Immutable snapshot path and canonical policy digest.
    :raises OSError: If the pointer cannot be opened or read.
    :raises ValueError: If the pointer is malformed or unsafe.
    """

    def unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate active snapshot pointer field")
            result[key] = value
        return result

    try:
        value = json.loads(_read_pointer(state).decode("utf-8"), object_pairs_hook=unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ValueError("invalid active snapshot pointer") from exc
    if not isinstance(value, dict) or set(value) != {"format", "format_version", "snapshot", "digest"}:
        raise ValueError("active snapshot pointer fields are missing or unknown")
    if value["format"] != _FORMAT or type(value["format_version"]) is not int or value["format_version"] != 1:
        raise ValueError("unsupported active snapshot pointer format or version")
    if type(value["snapshot"]) is not str:
        raise ValueError("active snapshot path must be a string")
    snapshot = Path(value["snapshot"])
    if not snapshot.is_absolute() or ".." in snapshot.parts or "\0" in value["snapshot"]:
        raise ValueError("active snapshot path must be absolute without '..' or NUL")
    if type(value["digest"]) is not str or _DIGEST.fullmatch(value["digest"]) is None:
        raise ValueError("active snapshot digest must be lowercase SHA-256")
    return snapshot, value["digest"]


def verify_active_snapshot(state: Path, snapshot: Path, policy: Policy) -> None:
    """Verify that this startup still names the protected active policy.

    :param state: Protected local daemon state directory.
    :param snapshot: Host snapshot path selected before sandbox entry.
    :param policy: Validated snapshot policy.
    :raises OSError: If the protected pointer cannot be read.
    :raises ValueError: If startup is stale or policy content does not match.
    """

    active_snapshot, active_digest = read_active_snapshot(state)
    expected = activation_document(snapshot, policy)
    if active_snapshot != snapshot or active_digest != expected["digest"]:
        raise ValueError("daemon startup selected a stale or mismatched active policy snapshot")


__all__ = ["activation_document", "read_active_snapshot", "verify_active_snapshot"]
