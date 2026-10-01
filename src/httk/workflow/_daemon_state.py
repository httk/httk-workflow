"""Protected durable state for the confined workspace daemon."""

import fcntl
import os
import re
import secrets
import sqlite3
import stat
import uuid
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Self
from urllib.parse import quote

from ._daemon_protocol import Request, Response, decode_request, decode_response, encode_request, encode_response
from ._daemon_protocol import request_digest as canonical_request_digest

_SCHEMA_VERSION = 1
_SCHEMA_ID = "httk-workspace-daemon-ledger-v1"
_STATES = frozenset({"received", "submitting", "submitted", "uncertain", "refused", "done"})
_JOB_ID = re.compile(r"[1-9][0-9]{0,19}\Z")
_CLUSTER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")
_HANDLE = re.compile(r"[0-9a-f]{32}\Z")


class ConflictError(RuntimeError):
    """A request identifier is already bound to different request content."""


class CapacityError(RuntimeError):
    """The enrollment's durable record or submission quota is exhausted."""


@dataclass(frozen=True, slots=True)
class Entry:
    """One validated durable daemon request and its current result.

    :param request: Canonical admitted request.
    :param state: Durable processing phase.
    :param handle: Opaque manager handle reserved for a start request.
    :param job_id: Protected scheduler job identifier after confirmed submission.
    :param cluster: Protected scheduler cluster after confirmed submission.
    :param response: Durable response, when processing has completed.
    """

    request: Request
    state: str
    handle: str | None
    job_id: str | None
    cluster: str | None
    response: Response | None


class Ledger:
    """Own one locked SQLite ledger for a daemon enrollment.

    :param directory: Pre-existing protected local state directory.
    :param workspace_id: Workspace identity pinned by protected policy.
    :param enrollment_id: Enrollment identity pinned by protected policy.
    :param initialize: Exclusively create and initialize a new ledger.
    :param max_records: Maximum durable request rows for this enrollment.
    :param max_submissions: Maximum admitted manager starts for this enrollment.
    :raises ValueError: If the path or configured bounds are invalid.
    :raises OSError: If protected files cannot be opened or the writer lock is held.
    :raises sqlite3.DatabaseError: If state is missing, corrupt, or incompatible.
    """

    def __init__(
        self,
        directory: Path,
        workspace_id: str,
        enrollment_id: str,
        *,
        initialize: bool = False,
        max_records: int = 4096,
        max_submissions: int = 128,
    ) -> None:
        if not isinstance(directory, Path) or not directory.is_absolute() or ".." in directory.parts:
            raise ValueError("ledger directory must be an absolute Path without '..'")
        if type(max_records) is not int or not 1 <= max_records <= 100_000:
            raise ValueError("max_records must be from 1 through 100000")
        if type(max_submissions) is not int or not 1 <= max_submissions <= max_records:
            raise ValueError("max_submissions must be from 1 through max_records")
        if type(workspace_id) is not str:
            raise ValueError("invalid workspace_id")
        try:
            if str(uuid.UUID(workspace_id)) != workspace_id:
                raise ValueError
        except (ValueError, AttributeError) as exc:
            raise ValueError("invalid workspace_id") from exc
        if type(enrollment_id) is not str or _HANDLE.fullmatch(enrollment_id) is None:
            raise ValueError("invalid enrollment_id")

        self.workspace_id = workspace_id
        self.enrollment_id = enrollment_id
        self.max_records = max_records
        self.max_submissions = max_submissions
        self._directory_fd = -1
        self._lock_fd = -1
        self._connection: sqlite3.Connection | None = None
        try:
            self._directory_fd = self._open_directory(directory)
            self._lock_fd = os.open(
                "daemon.lock",
                os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW,
                0o600,
                dir_fd=self._directory_fd,
            )
            if not stat.S_ISREG(os.fstat(self._lock_fd).st_mode):
                raise ValueError("daemon lock must be a regular file")
            try:
                fcntl.flock(self._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise OSError("daemon ledger is already locked") from exc

            if initialize:
                database_fd = os.open(
                    "ledger.sqlite3",
                    os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
                    0o600,
                    dir_fd=self._directory_fd,
                )
                os.close(database_fd)
            else:
                self._validate_database_file()

            database_path = f"/proc/self/fd/{self._directory_fd}/ledger.sqlite3"
            uri = f"file:{quote(database_path)}?mode=rw"
            self._connection = sqlite3.connect(uri, uri=True, isolation_level=None)
            self._configure()
            if initialize:
                self._initialize_schema()
            self._validate_schema()
        except BaseException:
            self.close()
            raise

    @staticmethod
    def _open_directory(path: Path) -> int:
        descriptor = os.open(path.anchor or "/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
        try:
            for component in path.parts[1:]:
                next_descriptor = os.open(
                    component,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
                    dir_fd=descriptor,
                )
                previous = descriptor
                descriptor = next_descriptor
                os.close(previous)
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise

    def _validate_database_file(self) -> None:
        descriptor = os.open(
            "ledger.sqlite3",
            os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK,
            dir_fd=self._directory_fd,
        )
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise ValueError("daemon ledger must be a regular file")
        finally:
            os.close(descriptor)

    def _db(self) -> sqlite3.Connection:
        connection = self._connection
        if connection is None:
            raise ValueError("ledger is closed")
        return connection

    def _configure(self) -> None:
        connection = self._db()
        mode = connection.execute("PRAGMA journal_mode=DELETE").fetchone()
        if mode is None or str(mode[0]).lower() != "delete":
            raise sqlite3.DatabaseError("daemon ledger requires the DELETE journal mode")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA trusted_schema=OFF")

    def _initialize_schema(self) -> None:
        connection = self._db()
        connection.execute("BEGIN IMMEDIATE")
        try:
            connection.execute(
                "CREATE TABLE metadata (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL) WITHOUT ROWID"
            )
            connection.execute(
                """
                CREATE TABLE requests (
                    request_id TEXT PRIMARY KEY NOT NULL,
                    request BLOB NOT NULL,
                    request_digest TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    state TEXT NOT NULL,
                    handle TEXT UNIQUE,
                    job_id TEXT,
                    cluster TEXT,
                    response BLOB
                ) WITHOUT ROWID
                """
            )
            connection.executemany(
                "INSERT INTO metadata(key, value) VALUES (?, ?)",
                (
                    ("schema", _SCHEMA_ID),
                    ("workspace_id", self.workspace_id),
                    ("enrollment_id", self.enrollment_id),
                ),
            )
            connection.execute(f"PRAGMA user_version={_SCHEMA_VERSION}")
            connection.execute("COMMIT")
            os.fsync(self._directory_fd)
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise

    def _validate_schema(self) -> None:
        connection = self._db()
        check = connection.execute("PRAGMA quick_check(1)").fetchone()
        if check != ("ok",):
            raise sqlite3.DatabaseError("daemon ledger integrity check failed")
        version = connection.execute("PRAGMA user_version").fetchone()
        if version != (_SCHEMA_VERSION,):
            raise sqlite3.DatabaseError("unsupported daemon ledger version")
        objects = {
            (row[0], row[1])
            for row in connection.execute("SELECT type, name FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'")
        }
        if objects != {("table", "metadata"), ("table", "requests")}:
            raise sqlite3.DatabaseError("invalid daemon ledger schema")
        schema_sql = {
            name: " ".join(sql.split())
            for name, sql in connection.execute(
                "SELECT name, sql FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
            if type(name) is str and type(sql) is str
        }
        if schema_sql != {
            "metadata": ("CREATE TABLE metadata (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL) WITHOUT ROWID"),
            "requests": (
                "CREATE TABLE requests ( request_id TEXT PRIMARY KEY NOT NULL, request BLOB NOT NULL, "
                "request_digest TEXT NOT NULL, operation TEXT NOT NULL, state TEXT NOT NULL, handle TEXT UNIQUE, "
                "job_id TEXT, cluster TEXT, response BLOB ) WITHOUT ROWID"
            ),
        }:
            raise sqlite3.DatabaseError("invalid daemon ledger schema")
        metadata_rows = connection.execute("SELECT key, value FROM metadata").fetchall()
        if any(type(key) is not str or type(value) is not str for key, value in metadata_rows):
            raise sqlite3.DatabaseError("invalid daemon ledger metadata")
        metadata = dict(metadata_rows)
        expected = {
            "schema": _SCHEMA_ID,
            "workspace_id": self.workspace_id,
            "enrollment_id": self.enrollment_id,
        }
        if len(metadata_rows) != len(expected) or metadata != expected:
            raise sqlite3.DatabaseError("daemon ledger identity mismatch")
        metadata_columns = connection.execute("PRAGMA table_info(metadata)").fetchall()
        request_columns = connection.execute("PRAGMA table_info(requests)").fetchall()
        if metadata_columns != [
            (0, "key", "TEXT", 1, None, 1),
            (1, "value", "TEXT", 1, None, 0),
        ] or request_columns != [
            (0, "request_id", "TEXT", 1, None, 1),
            (1, "request", "BLOB", 1, None, 0),
            (2, "request_digest", "TEXT", 1, None, 0),
            (3, "operation", "TEXT", 1, None, 0),
            (4, "state", "TEXT", 1, None, 0),
            (5, "handle", "TEXT", 0, None, 0),
            (6, "job_id", "TEXT", 0, None, 0),
            (7, "cluster", "TEXT", 0, None, 0),
            (8, "response", "BLOB", 0, None, 0),
        ]:
            raise sqlite3.DatabaseError("invalid daemon ledger schema")
        rows = connection.execute(
            "SELECT request_id, request, request_digest, operation, state, handle, job_id, cluster, response "
            "FROM requests ORDER BY request_id"
        )
        for count, row in enumerate(rows, 1):
            if count > 100_000:
                raise sqlite3.DatabaseError("daemon ledger exceeds its absolute record bound")
            self._entry(row)

    def __enter__(self) -> Self:
        """Return this open ledger."""

        self._db()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the connection and descriptors in reverse acquisition order."""

        self.close()

    @staticmethod
    def _blob(value: object, name: str) -> bytes:
        if type(value) is not bytes:
            raise sqlite3.DatabaseError(f"invalid stored {name}")
        return value

    def _entry(self, row: tuple[object, ...] | None) -> Entry | None:
        if row is None:
            return None
        request_id, request_wire, digest, operation, state, handle, job_id, cluster, response_wire = row
        request_bytes = self._blob(request_wire, "request")
        try:
            request = decode_request(request_bytes)
        except ValueError as exc:
            raise sqlite3.DatabaseError("invalid stored request") from exc
        if encode_request(request) != request_bytes:
            raise sqlite3.DatabaseError("noncanonical stored request")
        if (
            type(request_id) is not str
            or request.request_id != request_id
            or type(digest) is not str
            or canonical_request_digest(request) != digest
            or operation != request.operation
            or type(state) is not str
            or state not in _STATES
        ):
            raise sqlite3.DatabaseError("invalid stored request identity")
        if request.workspace_id != self.workspace_id or request.enrollment_id != self.enrollment_id:
            raise sqlite3.DatabaseError("invalid stored enrollment identity")
        if handle is not None and (type(handle) is not str or _HANDLE.fullmatch(handle) is None):
            raise sqlite3.DatabaseError("invalid stored manager handle")
        if (request.operation == "start_manager") != (handle is not None):
            raise sqlite3.DatabaseError("invalid stored manager handle")

        if state == "submitted":
            if (
                request.operation != "start_manager"
                or type(job_id) is not str
                or _JOB_ID.fullmatch(job_id) is None
                or type(cluster) is not str
                or _CLUSTER.fullmatch(cluster) is None
            ):
                raise sqlite3.DatabaseError("invalid stored scheduler identity")
        elif job_id is not None or cluster is not None:
            raise sqlite3.DatabaseError("unexpected stored scheduler identity")

        response: Response | None = None
        if response_wire is not None:
            response_bytes = self._blob(response_wire, "response")
            try:
                response = decode_response(response_bytes)
            except ValueError as exc:
                raise sqlite3.DatabaseError("invalid stored response") from exc
            if encode_response(response) != response_bytes:
                raise sqlite3.DatabaseError("noncanonical stored response")
            if (
                response.request_id != request.request_id
                or response.workspace_id != request.workspace_id
                or response.enrollment_id != request.enrollment_id
                or response.request_digest != digest
            ):
                raise sqlite3.DatabaseError("invalid stored response identity")
        if state in {"received", "submitting"}:
            if response is not None:
                raise sqlite3.DatabaseError("unexpected stored response")
        elif response is None:
            raise sqlite3.DatabaseError("missing stored response")
        expected_state = None if response is None else self._state_for_response(response)
        if expected_state is not None and state != expected_state:
            raise sqlite3.DatabaseError("stored response phase mismatch")
        if response is not None:
            self._validate_response(request, handle, response, stored=True)
        return Entry(request, state, handle, job_id, cluster, response)

    @staticmethod
    def _state_for_response(response: Response) -> str:
        return {
            "submitted": "submitted",
            "uncertain": "uncertain",
            "refused": "refused",
            "ready": "done",
            "status": "done",
            "cancel_requested": "done",
        }.get(response.outcome, "done")

    @staticmethod
    def _validate_response(request: Request, handle: object, response: Response, *, stored: bool) -> None:
        error = sqlite3.DatabaseError if stored else ValueError
        expected_handle = handle if request.operation == "start_manager" else request.handle
        if response.handle != expected_handle:
            raise error("response handle mismatch")
        permitted = {
            "health": {"ready", "refused"},
            "start_manager": {"submitted", "uncertain", "refused"},
            "manager_status": {"status", "refused"},
            "cancel_manager": {"cancel_requested", "refused"},
        }[request.operation]
        if response.outcome not in permitted:
            raise error("response operation mismatch")

    def _row(self, request_id: str) -> tuple[object, ...] | None:
        return (
            self._db()
            .execute(
                "SELECT request_id, request, request_digest, operation, state, handle, job_id, cluster, response "
                "FROM requests WHERE request_id=?",
                (request_id,),
            )
            .fetchone()
        )

    def admit(self, request: Request) -> Entry:
        """Durably admit a canonical request, or replay its existing entry.

        :param request: Validated request for this ledger identity.
        :return: New or existing durable entry.
        :raises ConflictError: If the request identifier names different content.
        :raises CapacityError: If an applicable cumulative quota is exhausted.
        """

        if type(request) is not Request:
            raise ValueError("request must be a Request")
        if request.workspace_id != self.workspace_id:
            raise ValueError("wrong workspace")
        if request.enrollment_id != self.enrollment_id:
            raise ValueError("wrong enrollment")
        digest = canonical_request_digest(request)
        connection = self._db()
        connection.execute("BEGIN IMMEDIATE")
        try:
            existing = self._entry(self._row(request.request_id))
            if existing is not None:
                if canonical_request_digest(existing.request) != digest:
                    raise ConflictError("request identifier conflict")
                connection.execute("COMMIT")
                return existing
            total = connection.execute("SELECT count(*) FROM requests").fetchone()
            if total is None or type(total[0]) is not int:
                raise sqlite3.DatabaseError("invalid daemon ledger count")
            if total[0] >= self.max_records:
                raise CapacityError("daemon record capacity exhausted")
            if request.operation == "start_manager":
                submissions = connection.execute(
                    "SELECT count(*) FROM requests WHERE operation='start_manager'"
                ).fetchone()
                if submissions is None or type(submissions[0]) is not int:
                    raise sqlite3.DatabaseError("invalid daemon submission count")
                if submissions[0] >= self.max_submissions:
                    raise CapacityError("daemon submission capacity exhausted")
                handle: str | None = secrets.token_hex(16)
            else:
                handle = None
            connection.execute(
                "INSERT INTO requests(request_id, request, request_digest, operation, state, handle) "
                "VALUES (?, ?, ?, ?, 'received', ?)",
                (request.request_id, encode_request(request), digest, request.operation, handle),
            )
            connection.execute("COMMIT")
            return Entry(request, "received", handle, None, None, None)
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise

    def begin_submission(self, request_id: str) -> Entry:
        """Commit submission intent before the scheduler is contacted."""

        connection = self._db()
        connection.execute("BEGIN IMMEDIATE")
        try:
            entry = self._entry(self._row(request_id))
            if entry is None or entry.request.operation != "start_manager" or entry.state != "received":
                raise ValueError("request is not a received manager start")
            changed = connection.execute(
                "UPDATE requests SET state='submitting' WHERE request_id=? AND state='received'",
                (request_id,),
            ).rowcount
            if changed != 1:
                raise sqlite3.DatabaseError("submission intent was not recorded")
            connection.execute("COMMIT")
            return Entry(entry.request, "submitting", entry.handle, None, None, None)
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise

    def finish(
        self,
        request_id: str,
        response: Response,
        *,
        job_id: str | None = None,
        cluster: str | None = None,
    ) -> Entry:
        """Commit a validated terminal response and optional scheduler identity."""

        if type(response) is not Response:
            raise ValueError("response must be a Response")
        connection = self._db()
        connection.execute("BEGIN IMMEDIATE")
        try:
            entry = self._entry(self._row(request_id))
            if entry is None:
                raise ValueError("unknown request")
            if entry.response is not None:
                raise ValueError("request already has a response")
            digest = canonical_request_digest(entry.request)
            if (
                response.request_id != entry.request.request_id
                or response.workspace_id != self.workspace_id
                or response.enrollment_id != self.enrollment_id
                or response.request_digest != digest
            ):
                raise ValueError("response identity mismatch")
            self._validate_response(entry.request, entry.handle, response, stored=False)
            state = self._state_for_response(response)
            if response.outcome in {"submitted", "uncertain"} and entry.state != "submitting":
                raise ValueError("submission response without committed intent")
            if response.outcome == "submitted":
                if (
                    type(job_id) is not str
                    or _JOB_ID.fullmatch(job_id) is None
                    or type(cluster) is not str
                    or _CLUSTER.fullmatch(cluster) is None
                ):
                    raise ValueError("invalid scheduler identity")
            elif job_id is not None or cluster is not None:
                raise ValueError("scheduler identity requires submitted outcome")
            changed = connection.execute(
                "UPDATE requests SET state=?, job_id=?, cluster=?, response=? WHERE request_id=? AND response IS NULL",
                (state, job_id, cluster, encode_response(response), request_id),
            ).rowcount
            if changed != 1:
                raise sqlite3.DatabaseError("request response was not recorded")
            connection.execute("COMMIT")
            return Entry(entry.request, state, entry.handle, job_id, cluster, response)
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise

    def lookup_manager(self, handle: str) -> Entry | None:
        """Return the manager-start entry for an opaque handle, if present."""

        if type(handle) is not str or _HANDLE.fullmatch(handle) is None:
            raise ValueError("invalid manager handle")
        row = (
            self._db()
            .execute(
                "SELECT request_id, request, request_digest, operation, state, handle, job_id, cluster, response "
                "FROM requests WHERE handle=? AND operation='start_manager'",
                (handle,),
            )
            .fetchone()
        )
        return self._entry(row)

    def recover(self) -> None:
        """Convert every interrupted submission intent to durable uncertainty."""

        connection = self._db()
        connection.execute("BEGIN IMMEDIATE")
        try:
            rows = connection.execute(
                "SELECT request_id, request, request_digest, operation, state, handle, job_id, cluster, response "
                "FROM requests WHERE state='submitting' ORDER BY request_id"
            ).fetchall()
            entries = [self._entry(row) for row in rows]
            for entry in entries:
                if entry is None or entry.handle is None:
                    raise sqlite3.DatabaseError("invalid interrupted submission")
                response = Response(
                    entry.request.request_id,
                    self.workspace_id,
                    self.enrollment_id,
                    canonical_request_digest(entry.request),
                    "uncertain",
                    handle=entry.handle,
                    reason="submission_unconfirmed",
                )
                changed = connection.execute(
                    "UPDATE requests SET state='uncertain', response=? "
                    "WHERE request_id=? AND state='submitting' AND response IS NULL",
                    (encode_response(response), entry.request.request_id),
                ).rowcount
                if changed != 1:
                    raise sqlite3.DatabaseError("interrupted submission recovery failed")
            connection.execute("COMMIT")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise

    def close(self) -> None:
        """Close SQLite and release the process lock; repeated calls are harmless."""

        connection = self._connection
        self._connection = None
        error: BaseException | None = None
        if connection is not None:
            try:
                connection.close()
            except BaseException as exc:
                error = exc
        lock_fd = self._lock_fd
        self._lock_fd = -1
        if lock_fd >= 0:
            try:
                os.close(lock_fd)
            except BaseException as exc:
                if error is None:
                    error = exc
        directory_fd = self._directory_fd
        self._directory_fd = -1
        if directory_fd >= 0:
            try:
                os.close(directory_fd)
            except BaseException as exc:
                if error is None:
                    error = exc
        if error is not None:
            raise error


__all__ = ["CapacityError", "ConflictError", "Entry", "Ledger"]
