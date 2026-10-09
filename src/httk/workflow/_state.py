"""The ``state.json`` document of a job: :class:`StateDoc`, strict and bounded.

``state.json`` is written only by the job's owner (through the kernel) and is
absent until the first claim. Read-only consumers use :func:`read_state_unowned`,
which tolerates a torn or absent file. Nested members are frozen on parse, so a
:class:`StateDoc` is immutable all the way down.
"""

import dataclasses
import json
import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Self

from httk.workflow import _fs
from httk.workflow.errors import FormatError, WorkflowError
from httk.workflow.models import canonical_uuid, validate_step

__all__ = [
    "MAX_STATE_BYTES",
    "STATE_FORMAT",
    "STATE_FORMAT_VERSION",
    "TERMINAL_STATES",
    "UNOWNED_STATES",
    "Release",
    "StateDoc",
    "decode_state",
    "encode_state",
    "read_state_unowned",
]

STATE_FORMAT = "httk-workflow-state"
STATE_FORMAT_VERSION = 1
#: The largest accepted ``state.json``, in encoded bytes.
MAX_STATE_BYTES = 1 << 20
#: The six states a job directory can rest in outside ``owned/``.
UNOWNED_STATES = ("ready", "waiting", "paused", "failed", "succeeded", "cancelled")
TERMINAL_STATES = ("succeeded", "failed", "cancelled")

_FAILURE_HISTORY_LIMIT = 16
_HISTORY_TAIL_LIMIT = 32
_PHASE_KINDS = ("idle", "launching", "running")
_ORIGINS = ("local", "exchange")
_ACTIVATION_REASONS = ("initial", "advance", "retry", "manual_continue", "override_step", "join")
_OWNER_ID = re.compile(r"[0-9a-f]{32}")

type Frozen = None | bool | int | float | str | tuple["Frozen", ...] | Mapping[str, "Frozen"]


def _freeze(value: object) -> Frozen:
    # JSON values only: objects become read-only mappings and arrays tuples.
    if isinstance(value, Mapping):
        items: dict[str, Frozen] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise FormatError("state.json object keys must be strings")
            items[key] = _freeze(item)
        return MappingProxyType(items)
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    raise FormatError(f"state.json cannot hold a {type(value).__name__}")


def _thaw(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    return value


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _count(mapping: Mapping[str, Frozen] | None, key: str) -> int:
    value = None if mapping is None else mapping.get(key)
    return value if type(value) is int else 0


def _encode(value: object) -> bytes:
    try:
        text = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise FormatError(f"state.json is not encodable: {exc}") from exc
    return text.encode("utf-8")


@dataclass(frozen=True)
class Release:
    """The release rule's record: where an owned job goes and which requests it applied.

    :param state: The unowned target state.
    :param priority: The target priority, ``0``–``999``.
    :param applied_requests: Request ids whose effect this release carries.
    """

    state: str
    priority: int
    applied_requests: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Validate the members."""

        if self.state not in UNOWNED_STATES:
            raise ValueError(f"not an unowned state: {self.state!r}")
        if type(self.priority) is not int or not 0 <= self.priority <= 999:
            raise ValueError(f"priority must be an integer 0-999: {self.priority!r}")
        object.__setattr__(self, "applied_requests", tuple(self.applied_requests))
        for request_id in self.applied_requests:
            canonical_uuid(request_id, "applied request id")


def _mapping_or_none(value: object, name: str) -> Mapping[str, Frozen] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise FormatError(f"state.json {name} must be an object or null")
    return value


def _records(value: object, name: str, limit: int | None = None) -> tuple[Mapping[str, Frozen], ...]:
    if not isinstance(value, tuple) or not all(isinstance(item, Mapping) for item in value):
        raise FormatError(f"state.json {name} must be a list of objects")
    if limit is not None and len(value) > limit:
        raise FormatError(f"state.json {name} holds more than {limit} entries")
    return value


def _release(value: object) -> Release | None:
    if value is None:
        return None
    if not isinstance(value, Mapping) or set(value) != {"state", "priority"}:
        raise FormatError("state.json release_to must be {state, priority} or null")
    state, priority = value["state"], value["priority"]
    if not isinstance(state, str) or isinstance(priority, bool) or not isinstance(priority, int):
        raise FormatError("state.json release_to must be {state: string, priority: integer}")
    try:
        return Release(state, priority)
    except ValueError as exc:
        raise FormatError(f"state.json release_to: {exc}") from exc


@dataclass(frozen=True)
class StateDoc:
    """The owner-written ``state.json`` of one job (format ``httk-workflow-state``, version 1).

    :param job_id: The job UUID.
    :param updated_at: When the document was written (UTC ISO 8601).
    :param owner_id: The writing owner, or ``None`` before any owner wrote it.
    :param phase: ``{kind: idle|launching|running, attempt_id}``.
    :param activation: The current activation, or ``None``.
    :param attempt: The current attempt, or ``None``.
    :param counters: ``{activations, attempts_total}``.
    :param resources: Resources chosen for the job, or ``None``.
    :param runner_steps: The runner's steps, or ``None``.
    :param failure: The current failure, or ``None``.
    :param failure_history: The last failures (at most 16).
    :param join: The pending join, or ``None``.
    :param observations: Recorded join observations.
    :param children: Published children.
    :param commit: The commit intent, or ``None``.
    :param release_to: The release intent, or ``None``.
    :param applied_requests: Applied request ids whose files may still exist.
    :param origin: ``"local"`` or ``"exchange"``.
    :param exchange_name: The exchange client's name for the job, or ``None``.
    :param detached: The detachment record, or ``None``.
    :param seal: The authoritative seal record, or ``None``.
    :param workflow_pin: The pinned installed workflow, or ``None``.
    :param history_tail: The last transitions (at most 32).
    """

    job_id: str
    updated_at: str
    owner_id: str | None
    phase: Mapping[str, Frozen]
    activation: Mapping[str, Frozen] | None
    attempt: Mapping[str, Frozen] | None
    counters: Mapping[str, Frozen]
    resources: Mapping[str, Frozen] | None
    runner_steps: tuple[Frozen, ...] | None
    failure: Mapping[str, Frozen] | None
    failure_history: tuple[Mapping[str, Frozen], ...]
    join: Mapping[str, Frozen] | None
    observations: tuple[Mapping[str, Frozen], ...]
    children: tuple[Mapping[str, Frozen], ...]
    commit: Mapping[str, Frozen] | None
    release_to: Release | None
    applied_requests: tuple[str, ...]
    origin: str
    exchange_name: str | None
    detached: Mapping[str, Frozen] | None
    seal: Mapping[str, Frozen] | None
    workflow_pin: Mapping[str, Frozen] | None
    history_tail: tuple[Mapping[str, Frozen], ...]

    @classmethod
    def empty(cls, job_id: str) -> Self:
        """Return the document of a job's first claim: idle, nothing recorded.

        :param job_id: The job UUID.
        :return: The new document.
        """

        return cls.from_mapping(
            {
                "format": STATE_FORMAT,
                "format_version": STATE_FORMAT_VERSION,
                "job_id": job_id,
                "updated_at": _now(),
                "owner_id": None,
                "phase": {"kind": "idle", "attempt_id": None},
                "activation": None,
                "attempt": None,
                "counters": {"activations": 0, "attempts_total": 0},
                "resources": None,
                "runner_steps": None,
                "failure": None,
                "failure_history": [],
                "join": None,
                "observations": [],
                "children": [],
                "commit": None,
                "release_to": None,
                "applied_requests": [],
                "origin": "local",
                "exchange_name": None,
                "detached": None,
                "seal": None,
                "workflow_pin": None,
                "history_tail": [],
            }
        )

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> Self:
        """Validate a decoded ``state.json`` strictly: every member present, nothing unknown.

        :param value: The decoded document.
        :return: The frozen document.
        :raises httk.workflow.errors.FormatError: If the document is malformed or too large.
        """

        if not isinstance(value, Mapping):
            raise FormatError("state.json must be an object")
        if len(_encode(value)) > MAX_STATE_BYTES:
            raise FormatError(f"state.json is larger than {MAX_STATE_BYTES} bytes")
        expected = {"format", "format_version"} | {field.name for field in dataclasses.fields(cls)}
        if set(value) != expected:
            raise FormatError(f"state.json members differ: {sorted(set(value) ^ expected)}")
        if value["format"] != STATE_FORMAT or value["format_version"] != STATE_FORMAT_VERSION:
            raise FormatError("state.json is not httk-workflow-state version 1")
        doc = {name: _freeze(item) for name, item in value.items()}
        phase = _mapping_or_none(doc["phase"], "phase")
        if phase is None or set(phase) != {"kind", "attempt_id"} or phase["kind"] not in _PHASE_KINDS:
            raise FormatError("state.json phase must be {kind: idle|launching|running, attempt_id}")
        counters = _mapping_or_none(doc["counters"], "counters")
        if counters is None:
            raise FormatError("state.json counters must be an object")
        owner_id = doc["owner_id"]
        if owner_id is not None and (not isinstance(owner_id, str) or not _OWNER_ID.fullmatch(owner_id)):
            raise FormatError("state.json owner_id must be 32 lowercase hex digits or null")
        if not isinstance(doc["updated_at"], str) or not doc["updated_at"]:
            raise FormatError("state.json updated_at must be a non-empty string")
        runner_steps = doc["runner_steps"]
        if runner_steps is not None and not isinstance(runner_steps, tuple):
            raise FormatError("state.json runner_steps must be a list or null")
        applied = doc["applied_requests"]
        if not isinstance(applied, tuple):
            raise FormatError("state.json applied_requests must be a list")
        if doc["origin"] not in _ORIGINS:
            raise FormatError("state.json origin must be 'local' or 'exchange'")
        exchange_name = doc["exchange_name"]
        return cls(
            job_id=canonical_uuid(doc["job_id"], "state.json job_id"),
            updated_at=doc["updated_at"],
            owner_id=owner_id,
            phase=phase,
            activation=_mapping_or_none(doc["activation"], "activation"),
            attempt=_mapping_or_none(doc["attempt"], "attempt"),
            counters=counters,
            resources=_mapping_or_none(doc["resources"], "resources"),
            runner_steps=runner_steps,
            failure=_mapping_or_none(doc["failure"], "failure"),
            failure_history=_records(doc["failure_history"], "failure_history", _FAILURE_HISTORY_LIMIT),
            join=_mapping_or_none(doc["join"], "join"),
            observations=_records(doc["observations"], "observations"),
            children=_records(doc["children"], "children"),
            commit=_mapping_or_none(doc["commit"], "commit"),
            release_to=_release(doc["release_to"]),
            applied_requests=tuple(canonical_uuid(item, "applied request id") for item in applied),
            origin=str(doc["origin"]),
            exchange_name=None if exchange_name is None else canonical_uuid(exchange_name, "exchange_name"),
            detached=_mapping_or_none(doc["detached"], "detached"),
            seal=_mapping_or_none(doc["seal"], "seal"),
            workflow_pin=_mapping_or_none(doc["workflow_pin"], "workflow_pin"),
            history_tail=_records(doc["history_tail"], "history_tail", _HISTORY_TAIL_LIMIT),
        )

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON-ready document, with ``format`` and ``format_version``.

        :return: A fresh mutable mapping.
        """

        members: dict[str, object] = {"format": STATE_FORMAT, "format_version": STATE_FORMAT_VERSION}
        for field in dataclasses.fields(self):
            members[field.name] = _thaw(getattr(self, field.name))
        release = self.release_to
        members["release_to"] = None if release is None else {"state": release.state, "priority": release.priority}
        return members

    def with_release(self, release: Release) -> Self:
        """Return the document that records *release* as the release intent.

        Sets ``release_to``, adds the release's request ids to ``applied_requests``
        and clears ``commit``, all in the one write the release rule makes.

        :param release: The release.
        :return: The new document.
        """

        applied = self.applied_requests + tuple(
            request_id for request_id in release.applied_requests if request_id not in self.applied_requests
        )
        return dataclasses.replace(
            self, release_to=Release(release.state, release.priority), applied_requests=applied, commit=None
        )

    def updated(self, **members: object) -> Self:
        """Return a validated copy with *members* replaced and ``updated_at`` set to now.

        :param **members: Document members and their new JSON values.
        :return: The new document.
        :raises httk.workflow.errors.FormatError: If the result is not a valid document.
        """

        mapping = self.as_mapping()
        mapping.update({name: _thaw(_freeze(value)) for name, value in members.items()})
        mapping["updated_at"] = _now()
        return type(self).from_mapping(mapping)

    def with_commit(self, commit: Mapping[str, object] | None) -> Self:
        """Return the document that records *commit* as the commit intent (``None`` clears it).

        :param commit: The commit intent of §3.3.
        :return: The new document.
        """

        return self.updated(commit=commit)

    def with_failure(self, failure: Mapping[str, object] | None, *, history_limit: int = 16) -> Self:
        """Return the document with *failure* as the current failure, also appended to the bounded history.

        :param failure: The failure mapping, or ``None`` to clear the current failure (history kept).
        :param history_limit: How many failures the history keeps, at most 16.
        :return: The new document.
        """

        history: tuple[object, ...] = self.failure_history
        if failure is not None:
            history = (*history, failure)[-history_limit:]
        return self.updated(failure=failure, failure_history=history)

    def next_activation(self, step: str, reason: str) -> Self:
        """Return the document of a new activation at *step*; it has no attempt yet.

        :param step: The step the activation runs.
        :param reason: Why: ``initial``, ``advance``, ``retry``, ``manual_continue``, ``override_step`` or ``join``.
        :return: The new document.
        :raises ValueError: For an unknown reason.
        """

        if reason not in _ACTIVATION_REASONS:
            raise ValueError(f"not an activation reason: {reason!r}")
        ordinal = _count(self.counters, "activations") + 1
        activation = {"id": str(uuid.uuid4()), "step": validate_step(step), "ordinal": ordinal, "reason": reason}
        return self.updated(activation=activation, attempt=None, counters={**self.counters, "activations": ordinal})

    def next_attempt(self, reason: str, *, unclean: bool) -> Self:
        """Return the document that records the next attempt of the current activation, not yet started.

        The next launch runs this attempt when its ``started_at`` is ``None``
        (see :meth:`with_phase`); otherwise it records a fresh one first.

        :param reason: Why the attempt runs (``launch``, ``retry``, ``owner_lost``, ``manual_continue``, ...).
        :param unclean: Whether the previous attempt ended uncleanly.
        :return: The new document.
        :raises ValueError: Without a current activation.
        """

        if self.activation is None:
            raise ValueError(f"job {self.job_id} has no activation to attempt")
        previous = self.attempt
        attempt = {
            "id": str(uuid.uuid4()),
            "ordinal": _count(previous, "ordinal") + 1,
            "reason": reason,
            "previous_attempt_id": None if previous is None else previous.get("id"),
            "started_at": None,
            "unclean": unclean,
        }
        counters = {**self.counters, "attempts_total": _count(self.counters, "attempts_total") + 1}
        return self.updated(attempt=attempt, counters=counters)

    def with_phase(self, kind: str, attempt_id: str | None) -> Self:
        """Return the document in phase *kind*; entering ``launching`` stamps the attempt's ``started_at``.

        :param kind: ``idle``, ``launching`` or ``running``.
        :param attempt_id: The current attempt's id; ``None`` exactly when *kind* is ``idle``.
        :return: The new document.
        :raises ValueError: For an unknown kind or an attempt id that is not the current attempt's.
        """

        if kind not in _PHASE_KINDS:
            raise ValueError(f"not a phase: {kind!r}")
        current = None if self.attempt is None else self.attempt.get("id")
        if (kind == "idle") != (attempt_id is None) or (attempt_id is not None and attempt_id != current):
            raise ValueError(f"phase {kind} cannot name attempt {attempt_id!r} (current: {current!r})")
        members: dict[str, object] = {"phase": {"kind": kind, "attempt_id": attempt_id}}
        if kind == "launching" and self.attempt is not None and self.attempt.get("started_at") is None:
            members["attempt"] = {**self.attempt, "started_at": _now()}
        return self.updated(**members)

    def with_observations(self, observations: Sequence[Mapping[str, object]]) -> Self:
        """Return the document with *observations* as the recorded join observations.

        :param observations: The observations (replace the current ones).
        :return: The new document.
        """

        return self.updated(observations=observations)

    def with_children(self, children: Sequence[Mapping[str, object]]) -> Self:
        """Return the document with *children* as the published children.

        :param children: Every published child (replace the current list).
        :return: The new document.
        """

        return self.updated(children=children)

    def with_history(self, event: str, /, **detail: object) -> Self:
        """Return the document with one ``{at, event, **detail}`` entry appended to the bounded ``history_tail``.

        :param event: The event name.
        :param **detail: Further JSON members of the entry (``from``, ``to``, ``owner_id``, ...).
        :return: The new document.
        """

        entry = {"at": _now(), "event": event, **detail}
        return self.updated(history_tail=(*self.history_tail, entry)[-_HISTORY_TAIL_LIMIT:])


def encode_state(doc: StateDoc) -> bytes:
    """Encode a document canonically for ``state.json``.

    :param doc: The document.
    :return: Canonical UTF-8 JSON bytes.
    :raises httk.workflow.errors.FormatError: If the encoding exceeds :data:`MAX_STATE_BYTES`.
    """

    data = _encode(doc.as_mapping())
    if len(data) > MAX_STATE_BYTES:
        raise FormatError(f"state.json would be larger than {MAX_STATE_BYTES} bytes")
    return data


def decode_state(data: bytes) -> StateDoc:
    """Decode and validate the bytes of a ``state.json``.

    :param data: The file content.
    :return: The document.
    :raises httk.workflow.errors.FormatError: If the content is not a valid document.
    """

    if len(data) > MAX_STATE_BYTES:
        raise FormatError(f"state.json is larger than {MAX_STATE_BYTES} bytes")
    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FormatError(f"state.json is not JSON: {exc}") from exc
    return StateDoc.from_mapping(value)


def read_state_unowned(path: Path) -> tuple[StateDoc | None, bool]:
    """Read a ``state.json`` without owning its job, tolerating an absent or damaged file.

    :param path: The ``state.json`` file.
    :return: The document (or ``None``) and whether the file exists but is damaged.
    """

    try:
        # Non-blocking: a FIFO a job planted at state.json must never stall a read-only consumer.
        data = _fs.read_bounded(_fs.loc(path), MAX_STATE_BYTES, nonblock=True)
    except (FileNotFoundError, NotADirectoryError):
        # The job moved away (or its parent was replaced) between listing and reading: absent.
        return None, False
    except (WorkflowError, OSError):
        # A symlink, a special file, an oversized or an unreadable file: damaged.
        return None, True
    if data is None:
        return None, False
    try:
        return decode_state(data), False
    except FormatError:
        return None, True
