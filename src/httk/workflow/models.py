"""Protocol models and validation."""

import dataclasses
import hashlib
import json
import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from ._util import (
    DEFAULT_VISIBILITY_DEADLINE_SECONDS,
    json_bytes,
    require_int,
    require_mapping,
    require_number,
    require_string,
)
from .errors import FormatError

CORE_PROFILE = "core-v2"
# core-v1 workspaces remain readable so an operator can inspect and export one,
# but their on-disk shapes predate mandatory spawn labels, enriched join
# observations, and shared runners, so nothing may mutate them in place.
READABLE_CORE_PROFILES = frozenset({"core-v1", CORE_PROFILE})
SUPPORTED_EXTENSIONS = frozenset({"transactional-data-v1", "detached-transfer-v1"})
# ``priority-bands-v1`` was specified and implemented, then withdrawn: it sharded
# ready/ into ten pNxx directories that every scan had to walk anyway, it was an
# irreversible choice made at init, and the exact priority was always in the
# marker basename regardless. A workspace that declares it cannot be interpreted
# by this implementation — its ready markers carry one extra path component that
# would be read as a placement — so it is refused rather than silently misread.
WITHDRAWN_EXTENSIONS = frozenset({"priority-bands-v1"})
RUNNER_SOURCES = frozenset({"payload", "workspace", "installed"})
PACKAGE_RUNNER_PREFIX = "pkg:"
STATE_KINDS = (
    "submitted",
    "ready",
    "claimed",
    "running",
    "committing",
    "cancelling",
    "relocating",
    "transferring",
    "waiting",
    "paused",
    "succeeded",
    "failed",
    "cancelled",
)
CORE_STATE_KINDS = tuple(kind for kind in STATE_KINDS if kind not in {"relocating", "transferring"})
TERMINAL_KINDS = frozenset({"succeeded", "failed", "cancelled"})
QUIESCENT_KINDS = frozenset({"submitted", "ready", "waiting", "paused", "failed", "succeeded", "cancelled"})

# Payload entries that belong to a runner rather than to the immutable job: the
# control directory of one attempt, and the state one runner keeps across the
# attempts and steps of a job. Both live inside the payload because they must
# travel with it, and neither is part of what the payload digest pins.
ATTEMPT_CONTROL_PREFIX = ".httk-attempt."
JOB_STATE_DIRECTORY = ".httk-job"
# Workspace policy: the tunables the specification calls "configured", stored
# once in format.json so that two implementations attaching the same workspace
# cannot disagree about them.
POLICY_KEYS = frozenset({"visibility_deadline_seconds", "lease_seconds", "journal_segment_bytes", "retention"})
RETENTION_KEYS = frozenset({"attempt_control_days", "journal_days", "trash_days"})
DEFAULT_LEASE_SECONDS = 900.0
DEFAULT_JOURNAL_SEGMENT_BYTES = 64 * 1024 * 1024
# A lease shorter than a second cannot be heartbeated honestly, and a deadline
# longer than a day is a hang rather than a filesystem waiting to settle.
MINIMUM_LEASE_SECONDS = 1.0
MAXIMUM_VISIBILITY_DEADLINE_SECONDS = 86400.0
MINIMUM_JOURNAL_SEGMENT_BYTES = 4096
MAXIMUM_JOURNAL_SEGMENT_BYTES = 1 << 40

# The serialized budget of the optional application-defined ``inputs`` object.
# Inputs describe one job; bulk data belongs in the payload or in transactional
# data, so a small bound keeps job.json readable and cheap to digest.
MAXIMUM_INPUTS_BYTES = 262144

_UUID_PATTERN = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_TAG_PATTERN = re.compile(r"[a-z0-9][a-z0-9._-]{0,47}")
_MARKER_PATTERN = re.compile(
    r"(?P<job_key>.+)\.p(?P<priority>[0-9]{3})\.g(?P<generation>[0-9a-z]+)\.(?P<record_ref>init|w[0-9a-f]{32}-s[0-9a-z]+-o[0-9a-z]+-l[0-9a-z]+-h[0-9a-f]{32})"
)
_LABEL_PATTERN = _TAG_PATTERN
_FAILURE_MEMBERS = frozenset({"code", "message", "details", "retryable"})
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
_MODULE_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*")
_UNSAFE_PATH_COMPONENTS = frozenset({"", ".", "..", ".httk-workflow"})


def canonical_uuid(value: object, name: str = "id") -> str:
    text = require_string(value, name)
    try:
        parsed = uuid.UUID(text)
    except ValueError as exc:
        raise FormatError(f"{name} must be a UUID") from exc
    canonical = str(parsed)
    if text != canonical:
        raise FormatError(f"{name} must use lowercase canonical UUID syntax")
    return canonical


def validate_label(value: object, name: str) -> str:
    text = require_string(value, name)
    if not _LABEL_PATTERN.fullmatch(text) or "--" in text:
        raise FormatError(f"{name} has invalid component syntax")
    return text


def is_payload_private(name: str) -> bool:
    """Report whether one payload entry name is runner-private scratch.

    A runner-private entry is excluded from every payload digest, so publishing
    an outcome or writing job state can never change the digest of a payload
    that a manager, a transfer, or a registration check must still recognize.
    """

    return name == JOB_STATE_DIRECTORY or name.startswith(ATTEMPT_CONTROL_PREFIX)


def validate_attempt_control(value: object, name: str = "attempt_control") -> str:
    """Validate one attempt-control directory name read from a state frame.

    The name is joined below a job payload to reach the control directory of an
    attempt, so it is exactly one relative component of the canonical
    ``.httk-attempt.<attempt-id>`` shape. Validating it before it is joined is
    what keeps a hostile or damaged frame from naming a path outside the job.
    """

    text = require_string(value, name)
    if not text.startswith(ATTEMPT_CONTROL_PREFIX):
        raise FormatError(f"{name} must name an {ATTEMPT_CONTROL_PREFIX}<attempt-id> directory")
    canonical_uuid(text[len(ATTEMPT_CONTROL_PREFIX) :], f"{name} attempt id")
    return text


def validate_inputs(value: object, name: str = "inputs") -> dict[str, object]:
    """Validate the optional application-defined ``inputs`` object of a job.

    The member is opaque to the protocol: only its shape, its key syntax, and
    its serialized size are checked. Its bytes are part of ``job.json`` and are
    therefore covered by the immutable job digest like every other member.
    """

    mapping = require_mapping(value, name)
    for key in mapping:
        if not isinstance(key, str) or not key:
            raise FormatError(f"{name} keys must be nonempty strings")
    try:
        size = len(json_bytes(mapping))
    except (TypeError, ValueError) as exc:
        raise FormatError(f"{name} must contain only JSON values: {exc}") from exc
    if size > MAXIMUM_INPUTS_BYTES:
        raise FormatError(
            f"{name} serializes to {size} bytes, which exceeds the {MAXIMUM_INPUTS_BYTES}-byte limit; "
            "put bulk content in the job payload or in transactional data instead"
        )
    return dict(mapping)


def validate_sha256(value: object, name: str) -> str:
    """Validate one lowercase hexadecimal SHA-256 digest string."""

    text = require_string(value, name)
    if not _SHA256_PATTERN.fullmatch(text):
        raise FormatError(f"{name} must be a lowercase hexadecimal SHA-256 digest")
    return text


def parse_package_runner(value: str) -> tuple[str, PurePosixPath] | None:
    """Split the reserved ``pkg:<module>/<resource>`` installed runner form.

    Return ``None`` when *value* is an ordinary relative runner path, so callers
    can treat the reserved form as one alternative spelling of ``runner.path``
    rather than as a separate protocol member.
    """

    if not value.startswith(PACKAGE_RUNNER_PREFIX):
        return None
    module, separator, resource = value[len(PACKAGE_RUNNER_PREFIX) :].partition("/")
    if not separator or not _MODULE_PATTERN.fullmatch(module):
        raise FormatError("runner.path must spell the reserved package form pkg:<module>/<resource>")
    relative = PurePosixPath(resource)
    if relative.is_absolute() or not relative.parts:
        raise FormatError("runner.path package resource must be a nonempty relative path")
    for part in relative.parts:
        if part in _UNSAFE_PATH_COMPONENTS or "\x00" in part:
            raise FormatError(f"invalid runner.path package resource component: {part!r}")
    return module, relative


def validate_runner_path(value: object, source: str) -> PurePosixPath:
    """Validate ``runner.path`` against the root implied by ``runner.source``.

    Every source resolves the same relative path below a different root: the job
    payload, the workspace runner store, or one configured installed-runner
    search path. The path must therefore stay below its root under every source,
    and only an installed runner may use the reserved ``pkg:`` form.
    """

    text = require_string(value, "runner.path")
    package = parse_package_runner(text)
    if package is not None:
        if source != "installed":
            raise FormatError("runner.path may use the pkg: form only when runner.source is installed")
        return PurePosixPath(text)
    path = PurePosixPath(text)
    if path.is_absolute() or not path.parts:
        raise FormatError(f"runner.path must be a nonempty path below the {source} runner root")
    for part in path.parts:
        if part in _UNSAFE_PATH_COMPONENTS or "\x00" in part:
            raise FormatError(f"runner.path must remain below the {source} runner root: {part!r}")
    return path


def job_digest(data: bytes) -> str:
    """Return the normative immutable job digest of stored ``job.json`` bytes.

    The digest of a job is the SHA-256 over the ``job.json`` file bytes exactly
    as submitted. Nothing rewrites or renormalizes those bytes, so the digest is
    reproducible by any implementation with only a hash utility.
    """

    return hashlib.sha256(data).hexdigest()


def validate_step(value: object, name: str = "step") -> str:
    text = require_string(value, name)
    if len(text.encode("utf-8")) > 128 or "/" in text or "\x00" in text:
        raise FormatError(f"{name} is not a valid step name")
    return text


def normalize_placement(value: str | PurePosixPath) -> PurePosixPath:
    placement = PurePosixPath(value)
    if placement.is_absolute() or not placement.parts:
        raise FormatError("placement must be a nonempty relative POSIX path")
    for part in placement.parts:
        if part in {"", ".", "..", ".httk-workflow"} or "\x00" in part:
            raise FormatError(f"invalid placement component: {part!r}")
        if len(part.encode()) > 255:
            raise FormatError(f"placement component is too long: {part!r}")
    return placement


def make_job_key(job_id: str, tag: str | None) -> str:
    return f"{tag}--{job_id}" if tag else job_id


def parse_job_key(value: str) -> tuple[str | None, str]:
    job_id = value[-36:]
    if not _UUID_PATTERN.fullmatch(job_id):
        raise FormatError(f"invalid job key UUID: {value!r}")
    if len(value) == 36:
        return None, job_id
    if value[-38:-36] != "--":
        raise FormatError(f"invalid job key separator: {value!r}")
    tag = validate_label(value[:-38], "job key tag")
    return tag, job_id


@dataclass(frozen=True)
class RetentionPolicy:
    """How long a workspace keeps the history it is allowed to collect.

    Every member is optional and means "no configured limit" when absent. The
    collector that acts on these numbers is a separate concern; the workspace
    only carries them so that every implementation attaching to it agrees on
    what may be removed and when.
    """

    attempt_control_days: float | None = None
    journal_days: float | None = None
    trash_days: float | None = None

    @classmethod
    def from_mapping(cls, value: object, name: str = "policy.retention") -> "RetentionPolicy":
        mapping = require_mapping(value, name)
        unsupported = sorted(set(mapping) - RETENTION_KEYS)
        if unsupported:
            raise FormatError(f"{name} has unsupported members: {', '.join(unsupported)}")

        def optional_days(key: str) -> float | None:
            raw = mapping.get(key)
            return None if raw is None else require_number(raw, f"{name}.{key}", minimum=0.0)

        return cls(
            attempt_control_days=optional_days("attempt_control_days"),
            journal_days=optional_days("journal_days"),
            trash_days=optional_days("trash_days"),
        )

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON representation, omitting unconfigured limits."""

        result: dict[str, object] = {}
        for key in sorted(RETENTION_KEYS):
            value = getattr(self, key)
            if value is not None:
                result[key] = value
        return result


@dataclass(frozen=True)
class WorkspacePolicy:
    """The tunables every implementation attaching to one workspace shares.

    These are workspace properties rather than per-process options: two
    managers on different hosts must agree on how long a marker may take to
    become visible and on how long an unheartbeaten lease means anything. They
    live in ``format.json`` beside the format and profile declarations, and a
    workspace written before this section existed simply reads as the defaults.
    """

    visibility_deadline_seconds: float = DEFAULT_VISIBILITY_DEADLINE_SECONDS
    lease_seconds: float = DEFAULT_LEASE_SECONDS
    journal_segment_bytes: int = DEFAULT_JOURNAL_SEGMENT_BYTES
    retention: RetentionPolicy = RetentionPolicy()

    @classmethod
    def from_mapping(cls, value: object, name: str = "policy") -> "WorkspacePolicy":
        """Validate one complete policy object, filling in absent members."""

        mapping = require_mapping(value, name)
        unsupported = sorted(set(mapping) - POLICY_KEYS)
        if unsupported:
            raise FormatError(
                f"{name} has unsupported members: {', '.join(unsupported)}; "
                f"the supported keys are {', '.join(sorted(POLICY_KEYS))}"
            )
        defaults = cls()
        deadline = mapping.get("visibility_deadline_seconds")
        lease = mapping.get("lease_seconds")
        segment = mapping.get("journal_segment_bytes")
        retention = mapping.get("retention")
        return cls(
            visibility_deadline_seconds=(
                defaults.visibility_deadline_seconds
                if deadline is None
                else require_number(
                    deadline,
                    f"{name}.visibility_deadline_seconds",
                    minimum=0.0,
                    maximum=MAXIMUM_VISIBILITY_DEADLINE_SECONDS,
                )
            ),
            lease_seconds=(
                defaults.lease_seconds
                if lease is None
                else require_number(lease, f"{name}.lease_seconds", minimum=MINIMUM_LEASE_SECONDS)
            ),
            journal_segment_bytes=(
                defaults.journal_segment_bytes
                if segment is None
                else require_int(
                    segment,
                    f"{name}.journal_segment_bytes",
                    minimum=MINIMUM_JOURNAL_SEGMENT_BYTES,
                    maximum=MAXIMUM_JOURNAL_SEGMENT_BYTES,
                )
            ),
            retention=(
                defaults.retention
                if retention is None
                else RetentionPolicy.from_mapping(retention, f"{name}.retention")
            ),
        )

    def as_mapping(self) -> dict[str, object]:
        """Return the complete JSON representation stored in ``format.json``."""

        return {
            "visibility_deadline_seconds": self.visibility_deadline_seconds,
            "lease_seconds": self.lease_seconds,
            "journal_segment_bytes": self.journal_segment_bytes,
            "retention": self.retention.as_mapping(),
        }

    def updated(self, changes: Mapping[str, object], name: str = "policy") -> "WorkspacePolicy":
        """Return this policy with *changes* applied and revalidated."""

        unsupported = sorted(set(changes) - POLICY_KEYS)
        if unsupported:
            raise FormatError(
                f"{name} has unsupported members: {', '.join(unsupported)}; "
                f"the supported keys are {', '.join(sorted(POLICY_KEYS))}"
            )
        return WorkspacePolicy.from_mapping({**self.as_mapping(), **dict(changes)}, name)


@dataclass(frozen=True)
class RetryPolicy:
    """The attempt budgets of one job and the failures it retries within them.

    Two independent rules make a failure retry-eligible, and both are bounded by
    exactly the same budgets:

    * ``retry_on`` lists failure codes. A manager-detected failure — a lost
      lease, a process failure, an unusable outcome — is retried when its code
      appears in this set.
    * A runner-declared failure published with ``retryable: true`` is retried
      whether or not its code appears in ``retry_on``, because the runner that
      produced the failure is the authority on whether repeating the attempt can
      help.

    Neither rule can exceed ``maximum_attempts_per_activation`` or
    ``maximum_total_attempts``: an exhausted budget always ends the job.
    """

    maximum_attempts_per_activation: int | None
    maximum_total_attempts: int | None
    maximum_activations: int | None
    retry_on: frozenset[str]

    @classmethod
    def from_mapping(cls, value: object) -> "RetryPolicy":
        mapping = require_mapping(value, "retry_policy")

        def optional_limit(name: str) -> int | None:
            raw = mapping.get(name)
            return None if raw is None else require_int(raw, f"retry_policy.{name}", minimum=1)

        retry_raw = mapping.get("retry_on", [])
        if not isinstance(retry_raw, Sequence) or isinstance(retry_raw, (str, bytes)):
            raise FormatError("retry_policy.retry_on must be an array")
        retry_on = frozenset(require_string(item, "retry_policy.retry_on item") for item in retry_raw)
        return cls(
            maximum_attempts_per_activation=optional_limit("maximum_attempts_per_activation"),
            maximum_total_attempts=optional_limit("maximum_total_attempts"),
            maximum_activations=optional_limit("maximum_activations"),
            retry_on=retry_on,
        )


@dataclass(frozen=True)
class Failure:
    """One canonical structured failure record.

    Every failure published by a runner, a bridge, or the manager itself uses
    exactly this shape: a stable machine ``code``, one human ``message``,
    optional structured ``details``, and the advisory ``retryable`` flag. Retry
    policy is keyed on ``code`` alone; ``retryable`` is recorded evidence and
    never a manager decision.
    """

    code: str
    message: str
    details: Mapping[str, object] | None = None
    retryable: bool = False

    def as_mapping(self) -> dict[str, object]:
        """Return the canonical JSON representation of this failure."""

        result: dict[str, object] = {"code": self.code, "message": self.message}
        if self.details is not None:
            result["details"] = dict(self.details)
        if self.retryable:
            result["retryable"] = True
        return result


def validate_failure(value: object, name: str = "failure") -> Failure:
    """Validate one published failure object."""

    mapping = require_mapping(value, name)
    unsupported = sorted(set(mapping) - _FAILURE_MEMBERS)
    if unsupported:
        raise FormatError(f"{name} has unsupported members: {', '.join(unsupported)}")
    code = require_string(mapping.get("code"), f"{name}.code")
    if len(code.encode("utf-8")) > 128 or "\x00" in code or any(character.isspace() for character in code):
        raise FormatError(f"{name}.code must be one short token without whitespace")
    message = require_string(mapping.get("message"), f"{name}.message")
    details_raw = mapping.get("details")
    details = None if details_raw is None else require_mapping(details_raw, f"{name}.details")
    retryable = mapping.get("retryable", False)
    if not isinstance(retryable, bool):
        raise FormatError(f"{name}.retryable must be a boolean")
    return Failure(
        code=code,
        message=message,
        details=None if details is None else dict(details),
        retryable=retryable,
    )


class _Unset:
    """One absent state-frame member, distinct from one whose value is null.

    A frame that carries ``"data_generation": null`` says something different
    from a frame that never mentions the member at all, and the difference has
    to survive a round trip, so absence needs a value of its own.
    """

    __slots__ = ()

    def __repr__(self) -> str:  # pragma: no cover - a debugging aid
        return "UNSET"


# Deliberately typed as ``Any`` so that every keyword of :meth:`StateFrame.of`
# can declare the type of its member and still default to absence. Nothing
# outside this module ever observes the value.
_UNSET: Any = _Unset()

#: The members every transition of one activation carries forward. They name
#: the activation and attempt a job is in, so a frame that dropped one of them
#: would lose the identity of work that is still running.
CARRIED_STATE_MEMBERS = (
    "step",
    "activation_id",
    "activation_ordinal",
    "attempt_id",
    "attempt_ordinal",
    "total_attempts",
    "data_generation",
    # The observed children of the join that started this activation are inputs
    # of the activation, exactly like its step: every attempt of it, including
    # one recovered from an abandoned claim or a retry, must see the same
    # children. A new activation resets the member.
    "join_summary",
    # The step set the runner of this job declared, carried forward until a
    # later outcome declares a different one.
    "runner_steps",
)


@dataclass(frozen=True)
class StateFrame:
    """The members of one state frame, typed for the manager that uses them.

    The frame is held exactly as it is on disk, so every member round-trips
    verbatim — including one written by a newer implementation, by an enabled
    extension, or by a workspace older than this code. What this implementation
    reads and writes goes through the typed accessors and through :meth:`of`,
    so a mistyped member name is a type error at the call site rather than a
    silently defaulted value at runtime.

    The envelope members ``format``, ``workspace_id``, ``job_id``, ``kind``,
    ``state_generation``, and their siblings belong to the transition that
    publishes a frame and are supplied by the workspace, never here.
    """

    members: Mapping[str, Any] = dataclasses.field(default_factory=dict)

    @classmethod
    def from_mapping(cls, value: object, name: str = "state frame") -> "StateFrame":
        """Read one stored state frame, keeping every member verbatim."""

        return cls(dict(require_mapping(value, name)))

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON representation, member for member."""

        return dict(self.members)

    def has(self, name: str) -> bool:
        """Report whether the frame carries *name* at all, null included."""

        return name in self.members

    @classmethod
    def of(
        cls,
        base: "StateFrame | None" = None,
        *,
        step: str = _UNSET,
        activation_id: str = _UNSET,
        activation_ordinal: int = _UNSET,
        attempt_id: str = _UNSET,
        attempt_ordinal: int = _UNSET,
        total_attempts: int = _UNSET,
        data_generation: int | None = _UNSET,
        join_summary: Sequence[object] | None = _UNSET,
        runner_steps: Sequence[str] = _UNSET,
        manager_id: str = _UNSET,
        writer_id: str = _UNSET,
        claim_id: str = _UNSET,
        attempt_control: str = _UNSET,
        lease_seconds: float = _UNSET,
        matched_pool: str = _UNSET,
        matched_capabilities: Sequence[str] = _UNSET,
        started_at: str = _UNSET,
        workdir: str = _UNSET,
        outcome_action: str = _UNSET,
        child_digests: Mapping[str, str] = _UNSET,
        child_labels: Mapping[str, str] = _UNSET,
        next_step: str = _UNSET,
        join: Mapping[str, object] = _UNSET,
        pause: object = _UNSET,
        failure: Mapping[str, object] = _UNSET,
        job_digest: str = _UNSET,
        unclean_restart: bool = _UNSET,
        unsafe_persistent_takeover: bool = _UNSET,
        takeover_evidence: Mapping[str, object] = _UNSET,
        cancellation: Mapping[str, object] = _UNSET,
        previous_attempt_id: str | None = _UNSET,
        operator: object = _UNSET,
        operator_key: object = _UNSET,
        operator_reason: object = _UNSET,
        request_id: object = _UNSET,
        revival_hazard: Mapping[str, object] = _UNSET,
        reason: str = _UNSET,
    ) -> "StateFrame":
        """Return *base* with the named members set, absent ones untouched.

        Every member a manager writes is one declared keyword, so the complete
        vocabulary of a state frame is visible in one signature and no call site
        can invent a member by misspelling one. Passing ``None`` writes the JSON
        null the protocol distinguishes from an absent member.
        """

        written: tuple[tuple[str, object], ...] = (
            ("step", step),
            ("activation_id", activation_id),
            ("activation_ordinal", activation_ordinal),
            ("attempt_id", attempt_id),
            ("attempt_ordinal", attempt_ordinal),
            ("total_attempts", total_attempts),
            ("data_generation", data_generation),
            ("join_summary", join_summary),
            ("runner_steps", runner_steps),
            ("manager_id", manager_id),
            ("writer_id", writer_id),
            ("claim_id", claim_id),
            ("attempt_control", attempt_control),
            ("lease_seconds", lease_seconds),
            ("matched_pool", matched_pool),
            ("matched_capabilities", matched_capabilities),
            ("started_at", started_at),
            ("workdir", workdir),
            ("outcome_action", outcome_action),
            ("child_digests", child_digests),
            ("child_labels", child_labels),
            ("next_step", next_step),
            ("join", join),
            ("pause", pause),
            ("failure", failure),
            ("job_digest", job_digest),
            ("unclean_restart", unclean_restart),
            ("unsafe_persistent_takeover", unsafe_persistent_takeover),
            ("takeover_evidence", takeover_evidence),
            ("cancellation", cancellation),
            ("previous_attempt_id", previous_attempt_id),
            ("operator", operator),
            ("operator_key", operator_key),
            ("operator_reason", operator_reason),
            ("request_id", request_id),
            ("revival_hazard", revival_hazard),
            ("reason", reason),
        )
        members: dict[str, Any] = {} if base is None else dict(base.members)
        for name, value in written:
            if value is not _UNSET:
                members[name] = value
        return cls(members)

    def carried(self) -> "StateFrame":
        """Return only the members every transition of this activation repeats."""

        return StateFrame({name: self.members[name] for name in CARRIED_STATE_MEMBERS if name in self.members})

    def select(self, names: Sequence[str]) -> "StateFrame":
        """Return only the named members this frame actually carries."""

        return StateFrame({name: self.members[name] for name in names if name in self.members})

    # -- typed reads ------------------------------------------------------

    def _string(self, name: str) -> str | None:
        value = self.members.get(name)
        return value if isinstance(value, str) and value else None

    def _integer(self, name: str) -> int | None:
        value = self.members.get(name)
        if isinstance(value, bool) or not isinstance(value, int):
            return None
        return value

    def _mapping(self, name: str) -> Mapping[str, object] | None:
        value = self.members.get(name)
        return value if isinstance(value, Mapping) else None

    def _flag(self, name: str) -> bool:
        return self.members.get(name) is True

    @property
    def step(self) -> str | None:
        return self._string("step")

    @property
    def activation_id(self) -> str | None:
        return self._string("activation_id")

    @property
    def activation_ordinal(self) -> int | None:
        return self._integer("activation_ordinal")

    @property
    def attempt_id(self) -> str | None:
        return self._string("attempt_id")

    @property
    def attempt_ordinal(self) -> int | None:
        return self._integer("attempt_ordinal")

    @property
    def total_attempts(self) -> int | None:
        return self._integer("total_attempts")

    @property
    def data_generation(self) -> int | None:
        return self._integer("data_generation")

    @property
    def join_summary(self) -> object:
        return self.members.get("join_summary")

    @property
    def manager_id(self) -> str | None:
        """Return the owning manager, refusing anything that is not one.

        The value is joined below ``managers/`` to reach a heartbeat, so a frame
        that does not name a canonical manager UUID is a protocol violation
        rather than a path to try.
        """

        value = self._string("manager_id")
        return None if value is None else canonical_uuid(value, "state.manager_id")

    @property
    def attempt_control(self) -> str | None:
        """Return the validated attempt-control component of this frame."""

        value = self._string("attempt_control")
        return None if value is None else validate_attempt_control(value, "state.attempt_control")

    @property
    def lease_seconds(self) -> float | None:
        value = self.members.get("lease_seconds")
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        return float(value)

    @property
    def started_at(self) -> str | None:
        return self._string("started_at")

    @property
    def workdir(self) -> str | None:
        return self._string("workdir")

    @property
    def next_step(self) -> str | None:
        return self._string("next_step")

    @property
    def join(self) -> Mapping[str, object] | None:
        return self._mapping("join")

    @property
    def failure(self) -> Mapping[str, object] | None:
        return self._mapping("failure")

    @property
    def pause(self) -> object:
        return self.members.get("pause")

    @property
    def cancellation(self) -> Mapping[str, object] | None:
        return self._mapping("cancellation")

    @property
    def child_digests(self) -> Mapping[str, object] | None:
        return self._mapping("child_digests")

    @property
    def previous_attempt_id(self) -> str | None:
        return self._string("previous_attempt_id")

    @property
    def unclean_restart(self) -> bool:
        return self._flag("unclean_restart")

    @property
    def unsafe_persistent_takeover(self) -> bool:
        return self._flag("unsafe_persistent_takeover")

    @property
    def reason(self) -> str | None:
        return self._string("reason")


@dataclass(frozen=True)
class JobDefinition:
    id: str
    tag: str | None
    name: str
    workflow: str
    runner_backend: str
    runner_source: str
    runner_path: PurePosixPath
    runner_sha256: str | None
    runner_arguments: tuple[str, ...]
    workdir_mode: str
    workdir_path: PurePosixPath
    data_mode: str
    initial_step: str
    priority: int
    claim_pool: str
    required_capabilities: frozenset[str]
    retry_policy: RetryPolicy
    resources: Mapping[str, object]
    inputs: Mapping[str, object]
    parent: Mapping[str, object] | None
    raw: Mapping[str, object]
    stored_digest: str | None = None

    @property
    def job_key(self) -> str:
        return make_job_key(self.id, self.tag)

    @property
    def digest(self) -> str:
        """Return the immutable job digest.

        Normatively the digest is :func:`job_digest` over the stored ``job.json``
        file bytes exactly as submitted, which is what every definition read
        through :meth:`from_bytes` carries. A definition composed in memory has
        no stored bytes yet, so its canonical serialization is hashed instead;
        the two agree as soon as that serialization is what gets written.
        """

        if self.stored_digest is not None:
            return self.stored_digest
        return job_digest(json_bytes(self.raw))

    @classmethod
    def from_bytes(cls, data: bytes, *, name: str = "job.json") -> "JobDefinition":
        """Parse stored ``job.json`` bytes, pinning the normative job digest."""

        try:
            value = json.loads(data.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise FormatError(f"cannot read JSON object {name}: {exc}") from exc
        if not isinstance(value, Mapping):
            raise FormatError(f"expected JSON object in {name}")
        job = cls.from_mapping(value)
        return dataclasses.replace(job, stored_digest=job_digest(data))

    @classmethod
    def from_path(cls, path: Path) -> "JobDefinition":
        """Read one stored ``job.json``, pinning the normative job digest."""

        try:
            data = path.read_bytes()
        except OSError as exc:
            raise FormatError(f"cannot read JSON object {path}: {exc}") from exc
        return cls.from_bytes(data, name=str(path))

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "JobDefinition":
        if value.get("format") != "httk-workflow-job" or value.get("format_version") != 1:
            raise FormatError("job format must be httk-workflow-job version 1")
        job_id = canonical_uuid(value.get("id"))
        tag_raw = value.get("tag")
        tag = None if tag_raw is None else validate_label(tag_raw, "tag")
        runner = require_mapping(value.get("runner"), "runner")
        runner_backend = validate_label(runner.get("backend", "path"), "runner.backend")
        arguments_raw = runner.get("arguments", [])
        if not isinstance(arguments_raw, Sequence) or isinstance(arguments_raw, (str, bytes)):
            raise FormatError("runner.arguments must be an array")
        arguments = tuple(require_string(item, "runner argument") for item in arguments_raw)
        runner_source = require_string(runner.get("source", "payload"), "runner.source")
        if runner_source not in RUNNER_SOURCES:
            raise FormatError(f"runner.source must be one of {', '.join(sorted(RUNNER_SOURCES))}")
        runner_path = validate_runner_path(runner.get("path"), runner_source)
        # A payload runner is already pinned by the immutable job digest, so a
        # second digest for it could only ever disagree with the payload. Every
        # shared runner lives outside the payload and must be pinned explicitly.
        if runner_source == "payload":
            if runner.get("sha256") is not None:
                raise FormatError("runner.sha256 is forbidden for a payload runner")
            runner_sha256 = None
        else:
            runner_sha256 = validate_sha256(runner.get("sha256"), "runner.sha256")
        workdir = require_mapping(value.get("workdir"), "workdir")
        workdir_mode = require_string(workdir.get("mode"), "workdir.mode")
        if workdir_mode not in {"persistent", "isolated"}:
            raise FormatError("workdir.mode must be persistent or isolated")
        workdir_path = PurePosixPath(require_string(workdir.get("path", "run"), "workdir.path"))
        if workdir_path.is_absolute() or ".." in workdir_path.parts or not workdir_path.parts:
            raise FormatError("workdir.path must remain below the job directory")
        data = require_mapping(value.get("data"), "data")
        data_mode = require_string(data.get("mode"), "data.mode")
        if data_mode not in {"none", "transactional"}:
            raise FormatError("data.mode must be none or transactional")
        claim = require_mapping(value.get("claim"), "claim")
        capabilities_raw = claim.get("required_capabilities", [])
        if not isinstance(capabilities_raw, Sequence) or isinstance(capabilities_raw, (str, bytes)):
            raise FormatError("claim.required_capabilities must be an array")
        capabilities = frozenset(validate_label(item, "capability") for item in capabilities_raw)
        resources = require_mapping(value.get("resources", {}), "resources")
        inputs_raw = value.get("inputs")
        inputs = {} if inputs_raw is None else validate_inputs(inputs_raw)
        parent_raw = value.get("parent")
        parent = None if parent_raw is None else require_mapping(parent_raw, "parent")
        return cls(
            id=job_id,
            tag=tag,
            name=require_string(value.get("name"), "name"),
            workflow=require_string(value.get("workflow"), "workflow"),
            runner_backend=runner_backend,
            runner_source=runner_source,
            runner_path=runner_path,
            runner_sha256=runner_sha256,
            runner_arguments=arguments,
            workdir_mode=workdir_mode,
            workdir_path=workdir_path,
            data_mode=data_mode,
            initial_step=validate_step(value.get("initial_step"), "initial_step"),
            priority=require_int(value.get("priority"), "priority", maximum=999),
            claim_pool=validate_label(claim.get("pool"), "claim.pool"),
            required_capabilities=capabilities,
            retry_policy=RetryPolicy.from_mapping(value.get("retry_policy", {})),
            resources=dict(resources),
            inputs=inputs,
            parent=None if parent is None else dict(parent),
            raw=dict(value),
        )


@dataclass(frozen=True)
class Marker:
    kind: str
    placement: PurePosixPath
    job_key: str
    priority: int
    generation: int
    record_ref: str
    path: Path

    @property
    def job_id(self) -> str:
        return parse_job_key(self.job_key)[1]

    @classmethod
    def from_path(cls, state_root: Path, path: Path) -> "Marker":
        relative = path.relative_to(state_root)
        if len(relative.parts) < 3:
            raise FormatError(f"marker has no placement: {path}")
        kind = relative.parts[0]
        if kind not in STATE_KINDS:
            raise FormatError(f"unknown state kind: {kind}")
        placement = normalize_placement(PurePosixPath(*relative.parts[1:-1]))
        match = _MARKER_PATTERN.fullmatch(relative.name)
        if match is None:
            raise FormatError(f"invalid marker basename: {relative.name}")
        job_key = match.group("job_key")
        parse_job_key(job_key)
        priority = int(match.group("priority"))
        generation = int(match.group("generation"), 36)
        if generation > (1 << 64) - 1:
            raise FormatError("state generation exceeds unsigned 64-bit range")
        return cls(kind, placement, job_key, priority, generation, match.group("record_ref"), path)


def marker_basename(job_key: str, priority: int, generation: int, record_ref: str) -> str:
    parse_job_key(job_key)
    if not 0 <= priority <= 999:
        raise FormatError("priority must be 0 through 999")
    if not 0 <= generation <= (1 << 64) - 1:
        raise FormatError("generation exceeds unsigned 64-bit range")
    generation_text = to_base36(generation)
    result = f"{job_key}.p{priority:03d}.g{generation_text}.{record_ref}"
    if len(result.encode("ascii")) > 213:
        raise FormatError("marker exceeds the core profile 213-byte budget")
    return result


def to_base36(value: int) -> str:
    if value < 0:
        raise ValueError("base-36 values cannot be negative")
    alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
    if value == 0:
        return "0"
    digits = ""
    while value:
        value, remainder = divmod(value, 36)
        digits = alphabet[remainder] + digits
    return digits
