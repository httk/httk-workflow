"""Protocol models and validation."""

import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from ._util import json_bytes, require_int, require_mapping, require_string
from .errors import FormatError

CORE_PROFILE = "core-v1"
SUPPORTED_EXTENSIONS = frozenset({"transactional-data-v1", "priority-bands-v1"})
STATE_KINDS = (
    "submitted",
    "ready",
    "claimed",
    "running",
    "committing",
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

_UUID_PATTERN = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_TAG_PATTERN = re.compile(r"[a-z0-9][a-z0-9._-]{0,47}")
_MARKER_PATTERN = re.compile(
    r"(?P<job_key>.+)\.p(?P<priority>[0-9]{3})\.g(?P<generation>[0-9a-z]+)\.(?P<record_ref>init|w[0-9a-f]{32}-s[0-9a-z]+-o[0-9a-z]+-l[0-9a-z]+-h[0-9a-f]{32})"
)
_LABEL_PATTERN = _TAG_PATTERN


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
class RetryPolicy:
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
class JobDefinition:
    id: str
    tag: str | None
    name: str
    workflow: str
    runner_backend: str
    runner_path: PurePosixPath
    runner_arguments: tuple[str, ...]
    workspace_mode: str
    workspace_path: PurePosixPath
    data_mode: str
    initial_step: str
    priority: int
    claim_pool: str
    required_capabilities: frozenset[str]
    retry_policy: RetryPolicy
    resources: Mapping[str, object]
    parent: Mapping[str, object] | None
    raw: Mapping[str, object]

    @property
    def job_key(self) -> str:
        return make_job_key(self.id, self.tag)

    @property
    def digest(self) -> str:
        import hashlib

        return hashlib.sha256(json_bytes(self.raw)).hexdigest()

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
        runner_path = PurePosixPath(require_string(runner.get("path"), "runner.path"))
        if runner_path.is_absolute() or ".." in runner_path.parts:
            raise FormatError("runner.path must remain below the job directory")
        workspace = require_mapping(value.get("workspace"), "workspace")
        workspace_mode = require_string(workspace.get("mode"), "workspace.mode")
        if workspace_mode not in {"persistent", "isolated"}:
            raise FormatError("workspace.mode must be persistent or isolated")
        workspace_path = PurePosixPath(require_string(workspace.get("path", "run"), "workspace.path"))
        if workspace_path.is_absolute() or ".." in workspace_path.parts or not workspace_path.parts:
            raise FormatError("workspace.path must remain below the job directory")
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
        parent_raw = value.get("parent")
        parent = None if parent_raw is None else require_mapping(parent_raw, "parent")
        return cls(
            id=job_id,
            tag=tag,
            name=require_string(value.get("name"), "name"),
            workflow=require_string(value.get("workflow"), "workflow"),
            runner_backend=runner_backend,
            runner_path=runner_path,
            runner_arguments=arguments,
            workspace_mode=workspace_mode,
            workspace_path=workspace_path,
            data_mode=data_mode,
            initial_step=validate_step(value.get("initial_step"), "initial_step"),
            priority=require_int(value.get("priority"), "priority", maximum=999),
            claim_pool=validate_label(claim.get("pool"), "claim.pool"),
            required_capabilities=capabilities,
            retry_policy=RetryPolicy.from_mapping(value.get("retry_policy", {})),
            resources=dict(resources),
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
    def from_path(cls, state_root: Path, path: Path, *, priority_bands: bool = False) -> "Marker":
        relative = path.relative_to(state_root)
        if len(relative.parts) < 3:
            raise FormatError(f"marker has no placement: {path}")
        kind = relative.parts[0]
        if kind not in STATE_KINDS:
            raise FormatError(f"unknown state kind: {kind}")
        position = 1
        if priority_bands and kind == "ready":
            expected_band = relative.parts[position]
            if not re.fullmatch(r"p[0-9]xx", expected_band):
                raise FormatError(f"invalid priority band: {expected_band}")
            position += 1
        placement = normalize_placement(PurePosixPath(*relative.parts[position:-1]))
        match = _MARKER_PATTERN.fullmatch(relative.name)
        if match is None:
            raise FormatError(f"invalid marker basename: {relative.name}")
        job_key = match.group("job_key")
        parse_job_key(job_key)
        priority = int(match.group("priority"))
        if priority_bands and kind == "ready" and relative.parts[1] != f"p{priority // 100}xx":
            raise FormatError("marker priority disagrees with ready priority band")
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
        raise FormatError("marker exceeds the core-v1 213-byte budget")
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
