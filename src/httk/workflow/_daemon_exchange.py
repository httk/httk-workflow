"""Content-blind broker movers between the client exchange and the workspace staging area.

Every poll reopens the directories component-wise without following symlinks from the
dedicated parent, moves eligible bundle directories with a plain same-mount ``rename`` to an
absent target name, copies the manager-written ``status.json`` out as bounded bytes, and publishes
``managers.json``. Bundle content is never opened.
"""

import errno
import json
import logging
import os
import re
import secrets
import stat
from contextlib import ExitStack
from datetime import UTC, datetime
from pathlib import Path

from ._daemon_mailbox import MAX_DIRECTORY_ENTRIES
from ._daemon_policy import _open_directory

_LOGGER = logging.getLogger(__name__)
_RACED = frozenset({errno.ENOENT, errno.EEXIST, errno.ENOTEMPTY, errno.ENOTDIR, errno.EISDIR})
_MAX_STATUS_BYTES = 1024 * 1024
_MAX_REPORTED = 4096
_ELIGIBLE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_RESERVED = frozenset(
    {"endpoint.json", "status.json", "managers.json", "rejected", "requests", "responses", "inbox", "outbox", "records"}
)
_STAGING = Path(".httk-workspace", "exchange")


def _rename(source: int, name: str, target: int, target_name: str) -> None:
    os.rename(name, target_name, src_dir_fd=source, dst_dir_fd=target)


def _component(name: str, label: str) -> str:
    if not isinstance(name, str) or name in ("", ".", "..") or "/" in name or "\0" in name:
        raise ValueError(f"{label} must be a single path component")
    return name


class ExchangeMover:
    """Move job bundles between the exchange and the workspace staging area.

    :param root: Absolute path of the dedicated parent holding the workspace and the exchange.
    :param exchange: Name of the exchange directory inside ``root``.
    :param workspace: Name of the workspace directory inside ``root``.
    :param enrollment_id: Enrollment identity published in ``managers.json``.
    :raises ValueError: If ``root`` is not absolute or a name is not a single component.
    """

    def __init__(self, root: Path, exchange: str, workspace: str, enrollment_id: str) -> None:
        if not isinstance(root, Path) or not root.is_absolute() or ".." in root.parts:
            raise ValueError("exchange root must be an absolute path without '..'")
        self._root = root
        self._exchange = Path(_component(exchange, "exchange name"))
        self._staging = Path(_component(workspace, "workspace name")) / _STAGING
        self._enrollment_id = enrollment_id
        self._reported: set[tuple[str, str]] = set()
        self._status: bytes | None = None
        self._managers: list[dict[str, str]] | None = None

    def _report(self, key: tuple[str, str], message: str, *arguments: object) -> None:
        # ponytail: one log line per key until 4096 distinct keys, then the memory resets and repeats are logged again.
        if key in self._reported:
            return
        if len(self._reported) >= _MAX_REPORTED:
            self._reported.clear()
        self._reported.add(key)
        _LOGGER.warning(message, *arguments)

    def _open(self, stack: ExitStack, root: int, path: Path) -> int | None:
        try:
            descriptor = _open_directory(path, root)
        except OSError as exc:
            self._report(("open", str(path)), "daemon_exchange_unavailable path=%s reason=%s", path, exc.strerror)
            return None
        stack.callback(os.close, descriptor)
        return descriptor

    def poll(self, managers: list[dict[str, str]]) -> None:
        """Run one bounded exchange pass.

        Missing or non-directory components are logged and skip only the directions that need them.

        :param managers: Ledger manager rows to publish in ``managers.json``.
        """

        with ExitStack() as stack:
            try:
                root = _open_directory(self._root)
            except OSError as exc:
                self._report(
                    ("open", str(self._root)), "daemon_exchange_unavailable path=%s reason=%s", self._root, exc.strerror
                )
                return
            stack.callback(os.close, root)
            exchange_inbox, exchange_outbox, exchange_rejected = (
                self._open(stack, root, self._exchange / name) for name in ("inbox", "outbox", "outbox/rejected")
            )
            staging_inbox, staging_outbox, staging_rejected = (
                self._open(stack, root, self._staging / name) for name in ("inbox", "outbox", "outbox/rejected")
            )
            self._move("inbox", exchange_inbox, staging_inbox)
            self._move("outbox", staging_outbox, exchange_outbox)
            self._move("rejected", staging_rejected, exchange_rejected)
            if exchange_outbox is not None:
                if staging_outbox is not None:
                    self._copy_status(staging_outbox, exchange_outbox)
                self._publish_managers(exchange_outbox, managers)

    def _move(self, label: str, source: int | None, target: int | None) -> None:
        if source is None or target is None:
            return
        try:
            with os.scandir(source) as entries:
                names = [entry.name for _, entry in zip(range(MAX_DIRECTORY_ENTRIES), entries, strict=False)]
        except OSError as exc:
            self._report((label, ""), "daemon_exchange_unavailable direction=%s reason=%s", label, exc.strerror)
            return
        for name in sorted(names):
            if name.startswith(".") or name in _RESERVED:
                continue
            if _ELIGIBLE.fullmatch(name) is None:
                self._report(
                    (label, name), "daemon_exchange_skipped direction=%s name=%r reason=invalid_name", label, name
                )
                continue
            try:
                information = os.stat(name, dir_fd=source, follow_symlinks=False)
            except FileNotFoundError:
                continue
            except OSError as exc:
                self._report(
                    (label, name), "daemon_exchange_skipped direction=%s name=%s reason=%s", label, name, exc.strerror
                )
                continue
            if not stat.S_ISDIR(information.st_mode):
                self._report(
                    (label, name), "daemon_exchange_skipped direction=%s name=%s reason=not_directory", label, name
                )
                continue
            try:
                os.stat(name, dir_fd=target, follow_symlinks=False)
            except FileNotFoundError:
                pass
            except OSError as exc:
                self._report(
                    (label, name), "daemon_exchange_skipped direction=%s name=%s reason=%s", label, name, exc.strerror
                )
                continue
            else:
                continue  # an existing target is retried on the next poll
            # ponytail: no-replace renames are unavailable on NFS/Lustre/GPFS. A directory rename can replace only
            # a target that is an empty directory created after the check above, so that race loses no data.
            try:
                _rename(source, name, target, name)
            except OSError as exc:
                if exc.errno not in _RACED:
                    self._report(
                        (label, name),
                        "daemon_exchange_skipped direction=%s name=%s reason=%s",
                        label,
                        name,
                        exc.strerror,
                    )
                continue
            self._settle(label, name, target)

    def _settle(self, label: str, name: str, target: int) -> None:
        """Quarantine a moved entry that the source owner swapped for a non-directory after the check."""

        try:
            if stat.S_ISDIR(os.stat(name, dir_fd=target, follow_symlinks=False).st_mode):
                _LOGGER.info("daemon_exchange_moved direction=%s name=%s", label, name)
                return
        except FileNotFoundError:
            return
        quarantine = f".quarantine-{secrets.token_hex(16)}"
        try:
            _rename(target, name, target, quarantine)
        except OSError as exc:
            self._report(
                (label, name),
                "daemon_exchange_quarantine_failed direction=%s name=%s reason=%s",
                label,
                name,
                exc.strerror,
            )
            return
        self._report(
            (label, name),
            "daemon_exchange_quarantined direction=%s name=%s as=%s reason=not_directory",
            label,
            name,
            quarantine,
        )

    def _copy_status(self, source: int, target: int) -> None:
        try:
            descriptor = os.open(
                "status.json", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=source
            )
        except FileNotFoundError:
            return
        except OSError as exc:
            self._report(("status", "open"), "daemon_exchange_status_skipped reason=%s", exc.strerror)
            return
        try:
            information = os.fstat(descriptor)
            if not stat.S_ISREG(information.st_mode) or information.st_size > _MAX_STATUS_BYTES:
                self._report(("status", "kind"), "daemon_exchange_status_skipped reason=not_bounded_regular_file")
                return
            data = bytearray()
            while len(data) <= _MAX_STATUS_BYTES:
                chunk = os.read(descriptor, _MAX_STATUS_BYTES + 1 - len(data))
                if not chunk:
                    break
                data.extend(chunk)
        except OSError as exc:
            self._report(("status", "read"), "daemon_exchange_status_skipped reason=%s", exc.strerror)
            return
        finally:
            os.close(descriptor)
        if len(data) > _MAX_STATUS_BYTES:
            self._report(("status", "size"), "daemon_exchange_status_skipped reason=too_large")
            return
        content = bytes(data)
        if content != self._status and self._replace(target, "status.json", ".status-", content):
            self._status = content

    def _publish_managers(self, target: int, managers: list[dict[str, str]]) -> None:
        if managers == self._managers:
            return
        document = {
            "format": "httk-workspace-daemon-managers",
            "format_version": 1,
            "enrollment_id": self._enrollment_id,
            "generated_at": datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z"),
            "managers": managers,
        }
        data = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
        if self._replace(target, "managers.json", ".managers-", data):
            self._managers = managers

    def _replace(self, directory: int, name: str, prefix: str, data: bytes) -> bool:
        """Install ``data`` as ``name`` through a fresh broker-owned dot-named temporary."""

        temporary = f"{prefix}{secrets.token_hex(16)}"
        try:
            descriptor = os.open(
                temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=directory
            )
            try:
                try:
                    view = memoryview(data)
                    while view:
                        view = view[os.write(descriptor, view) :]
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
                os.rename(temporary, name, src_dir_fd=directory, dst_dir_fd=directory)
            except BaseException:
                try:
                    os.unlink(temporary, dir_fd=directory)
                except OSError:
                    pass
                raise
        except OSError as exc:
            self._report(("publish", name), "daemon_exchange_publish_failed name=%s reason=%s", name, exc.strerror)
            return False
        return True


__all__ = ["ExchangeMover"]
