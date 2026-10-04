"""The workspace-daemon exchange pass of ``httk workflow manager run --exchange``.

Each pass adopts the job directories the daemon broker staged in
``WORKSPACE/.httk-workspace/exchange/inbox``, ejects finished jobs into the
staging ``outbox``, keeps a persistent record of every refusal and eject error
under ``records``, and publishes a passive ``outbox/status.json``. Everything in
the staging directories may have been written by payload code of this same
workspace, so a pass reads it defensively and never raises into the manager.
"""

import json
import logging
import os
import re
import stat
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ._util import json_bytes, timestamp_seconds, write_json_atomic
from .errors import WorkflowError
from .models import STATE_KINDS, TERMINAL_KINDS, Marker
from .transfers import (
    _eject_tree_of,
    _require_tree_boundary,
    _unresolved_join_reference,
    _waiting_parent_map,
    exchange_staging,
    recover_interrupted_transfers,
)
from .workspace import Workspace

_LOGGER = logging.getLogger(__name__)

__all__ = ["exchange_pass"]

_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_RESERVED = frozenset(
    {"endpoint.json", "status.json", "managers.json", "rejected", "requests", "responses", "inbox", "outbox", "records"}
)
_REASON_LIMIT = 1000
_RECORD_LIMIT = 16 * 1024
_STATUS_LIMIT = 1024 * 1024
#: The least time between two recovery, eject or status steps of one workspace;
#: adoption runs on every pass, so an adopted job is claimed in the same tick.
_STEP_INTERVAL = 10.0
#: How long a job stays terminal before it is ejected, so the attempt process a
#: manager commits before reaping has exited before its payload moves.
_EJECT_GRACE_SECONDS = 60.0
#: When this process last ran each rate-limited step, by staging directory and step.
_last_run: dict[tuple[Path, str], float] = {}


def exchange_pass(workspace: Workspace, *, now: float) -> None:
    """Run one exchange pass: recovery, adoption, ejection, then the status document.

    Adoption runs on every pass; recovery, ejection and the status document at
    most once per 10 seconds each. Each step logs and swallows its own failures,
    so a broken bundle or a tampered staging directory never stops the manager
    calling this.

    :param workspace: The workspace the manager serves.
    :param now: The current epoch second, which rate-limits the steps and dates the status.
    """

    staging = exchange_staging(workspace)
    steps = (
        ("recovery", lambda: recover_interrupted_transfers(workspace)),
        ("adopt", lambda: _adopt_staged(workspace)),
        ("eject", lambda: _eject_finished(workspace, now)),
        ("status", lambda: _publish_status(workspace, now)),
    )
    for name, step in steps:
        if name != "adopt":
            last = _last_run.get((staging, name))
            if last is not None and 0.0 <= now - last < _STEP_INTERVAL:
                continue
            _last_run[(staging, name)] = now
        try:
            step()
        except Exception:
            _LOGGER.exception("exchange %s step failed", name)


def _publish_status(workspace: Workspace, now: float) -> None:
    outbox = exchange_staging(workspace) / "outbox"
    if _directory(outbox):
        write_json_atomic(outbox / "status.json", _status(workspace, now))


def _directory(path: Path) -> bool:
    """Create one staging directory if missing; report whether it is a real directory."""

    try:
        path.mkdir(parents=True, exist_ok=True)
        if stat.S_ISDIR(os.lstat(path).st_mode):
            return True
    except OSError:
        pass
    _LOGGER.warning("exchange staging %s is not a directory; skipping it", path)
    return False


def _record(workspace: Workspace, name: str, document: dict[str, str]) -> None:
    """Write one record, unless it already says the same."""

    path = exchange_staging(workspace) / "records" / f"{name}.json"
    document = {**document, "reason": document["reason"][:_REASON_LIMIT]}
    existing = _read_record(path)
    if existing is not None and all(existing.get(key) == value for key, value in document.items()):
        return
    write_json_atomic(path, {**document, "at": _timestamp(None)}, durable=workspace.durable)


def _adopt_staged(workspace: Workspace) -> None:
    staging = exchange_staging(workspace)
    inbox, rejected = staging / "inbox", staging / "outbox" / "rejected"
    if not (_directory(inbox) and _directory(rejected) and _directory(staging / "records")):
        return
    for name in sorted(os.listdir(inbox)):
        if name.startswith(".") or name in _RESERVED or _NAME.fullmatch(name) is None:
            continue
        path = inbox / name
        try:
            if not stat.S_ISDIR(os.lstat(path).st_mode):
                # Never a job directory, and the broker forwards only directories,
                # so it would pin its name in rejected/ for good.
                path.unlink()
                _LOGGER.warning("removed staged entry %s: not a job directory", name)
                _record(
                    workspace,
                    f"rejected-{name}",
                    {"name": name, "reason": "not a job directory (a symlink, file or special file); removed"},
                )
                continue
        except FileNotFoundError:
            continue
        try:
            workspace.adopt(path)
        except Exception as exc:
            if not os.path.lexists(path):
                continue  # another manager took it
            reason = f"{type(exc).__name__}: {exc}"
            _LOGGER.warning("refused staged job directory %s: %s", name, reason)
            if os.path.lexists(rejected / name):
                _LOGGER.warning("rejected/%s is still taken; %s stays in the inbox", name, name)
            else:
                try:
                    # ponytail: check-then-rename, not RENAME_NOREPLACE; the workspace is single-principal.
                    os.rename(path, rejected / name)
                except FileNotFoundError:
                    continue
            _record(workspace, f"rejected-{name}", {"name": name, "reason": reason})
        else:
            _LOGGER.info("adopted staged job directory %s", name)
            (staging / "records" / f"rejected-{name}.json").unlink(missing_ok=True)


def _ejectable(workspace: Workspace, marker: Marker, waiting: dict[str, set[str]], now: float) -> bool:
    """Report whether a terminal job may leave now; one that may not is pending, not an error.

    The terminal frame's ``created_at`` dates the transition: a marker is moved
    by rename, which keeps its mtime from whenever the file was first written.
    """

    try:
        finished = timestamp_seconds(str(workspace.read_state(marker)["created_at"]))
    except (WorkflowError, OSError, KeyError, ValueError):
        return False
    if now - finished < _EJECT_GRACE_SECONDS:
        return False
    try:
        if _unresolved_join_reference(workspace, marker, waiting):
            return False
        _require_tree_boundary(workspace, marker, with_tree=True)
        _eject_tree_of(workspace, marker, waiting)
    except ValueError:
        return False
    return True


def _eject_finished(workspace: Workspace, now: float) -> None:
    staging = exchange_staging(workspace)
    outbox, records = staging / "outbox", staging / "records"
    if not (_directory(outbox) and _directory(records)):
        return
    waiting = _waiting_parent_map(workspace)
    # ponytail: every terminal job is re-checked each pass; index pending ones if workspaces grow large.
    for marker in sorted(workspace.scan_markers(tuple(TERMINAL_KINDS)), key=lambda item: item.job_key):
        if not _ejectable(workspace, marker, waiting, now):
            continue
        record = records / f"eject-{marker.job_id}.json"
        try:
            workspace.eject(marker.job_id, outbox)
        except FileExistsError:
            continue  # the previous copy is still waiting for the broker
        except Exception as exc:
            reason = f"{type(exc).__name__}: {exc}"
            _LOGGER.warning("cannot eject finished job %s: %s", marker.job_key, reason)
            _record(workspace, f"eject-{marker.job_id}", {"job_id": marker.job_id, "reason": reason})
        else:
            _LOGGER.info("ejected finished job %s to the exchange", marker.job_key)
            record.unlink(missing_ok=True)


def _read_record(path: Path) -> dict[str, Any] | None:
    """Read one record without following a symlink or blocking on a FIFO; ``None`` when unusable."""

    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_size > _RECORD_LIMIT:
                return None
            value = json.loads(os.read(descriptor, _RECORD_LIMIT + 1))
        finally:
            os.close(descriptor)
    except (OSError, ValueError, RecursionError):
        return None
    return value if isinstance(value, dict) else None


def _records(records: Path, prefix: str, key: str) -> Iterator[dict[str, str]]:
    for path in sorted(records.glob(f"{prefix}-*.json")):
        value = _read_record(path)
        if value is not None and isinstance(value.get(key), str) and isinstance(value.get("reason"), str):
            yield {key: value[key], "reason": value["reason"][:_REASON_LIMIT]}


def _timestamp(now: float | None) -> str:
    moment = datetime.now(UTC) if now is None else datetime.fromtimestamp(now, UTC)
    return moment.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _status(workspace: Workspace, now: float) -> dict[str, Any]:
    """Render the status document from markers and records, bounded to 1 MiB."""

    records = exchange_staging(workspace) / "records"
    markers = sorted(workspace.scan_markers(STATE_KINDS), key=lambda item: item.job_key)
    live = {item.job_id for item in markers}
    for path in records.glob("eject-*.json"):
        if path.name[len("eject-") : -len(".json")] not in live:
            # The job left since (a later pass or recovery finished its ejection).
            path.unlink(missing_ok=True)
    document: dict[str, Any] = {
        "format": "httk-workspace-daemon-status",
        "format_version": 1,
        "workspace_id": workspace.workspace_id,
        "generated_at": _timestamp(now),
        "jobs": [{"job_id": item.job_id, "job_key": item.job_key, "state": item.kind} for item in markers],
        "rejected": list(_records(records, "rejected", "name")),
        "eject_errors": [item for item in _records(records, "eject", "job_id") if item["job_id"] in live],
        "truncated": False,
    }
    # Drop from the end of each list in turn (records only after every job is
    # gone), keeping the longest prefix that fits.
    for key in ("jobs", "eject_errors", "rejected"):
        if len(json_bytes(document)) + 1 <= _STATUS_LIMIT:
            break
        document["truncated"] = True
        items, low, high = document[key], 0, len(document[key])
        while low < high:
            middle = (low + high + 1) // 2
            document[key] = items[:middle]
            if len(json_bytes(document)) + 1 <= _STATUS_LIMIT:
                low = middle
            else:
                high = middle - 1
        document[key] = items[:low]
    return document
