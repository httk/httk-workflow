"""The immutable ``job.json`` of a job (format ``httk-workflow-job``, version 3): :class:`JobDefinition`.

Version 3 carries no runner, workdir, data, ``requires`` or ``calls``: the
installed workflow supplies them. ``placement`` is authoritative and immutable.
The job digest is the SHA-256 of the stored bytes, exactly as submitted.
"""

import dataclasses
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Self

from httk.workflow import _fs
from httk.workflow._state import Frozen, _freeze, _thaw
from httk.workflow._util import json_bytes, require_int, require_mapping, require_string
from httk.workflow.errors import FormatError
from httk.workflow.models import (
    RetryPolicy,
    _validate_step_resources,
    canonical_uuid,
    check_job_placement,
    job_digest,
    make_job_key,
    parse_job_key,
    parse_placement_text,
    placement_text,
    validate_declarations,
    validate_declared,
    validate_environment,
    validate_label,
    validate_parameters,
    validate_resources,
    validate_step,
)

__all__ = ["JOB_FORMAT", "JOB_FORMAT_VERSION", "MAX_JOB_BYTES", "JobDefinition"]

JOB_FORMAT = "httk-workflow-job"
JOB_FORMAT_VERSION = 3
#: The largest accepted ``job.json``, in bytes (the legacy bound).
MAX_JOB_BYTES = 1 << 20

_MEMBERS = frozenset(
    {
        "format",
        "format_version",
        "id",
        "tag",
        "name",
        "placement",
        "workflow",
        "initial_step",
        "priority",
        "claim",
        "retry_policy",
        "resources",
        "step_resources",
        "parameters",
        "declarations",
        "declared",
        "environment",
        "parent",
        "seal_succeeded",
    }
)
_RETRY_MEMBERS = frozenset(
    {"maximum_attempts_per_activation", "maximum_total_attempts", "maximum_activations", "retry_on"}
)
_PARENT_MEMBERS = frozenset({"workspace_id", "job_id", "job_key", "placement", "activation_id", "spawn_id"})


def _exact(
    value: object, name: str, members: frozenset[str], *, optional: frozenset[str] = frozenset()
) -> Mapping[str, object]:
    mapping = require_mapping(value, name)
    if not (members - optional) <= set(mapping) <= members:
        raise FormatError(f"{name} members differ: {sorted(set(mapping) ^ members)}")
    return mapping


def _frozen(value: object) -> Mapping[str, Frozen]:
    frozen = _freeze(value)
    assert isinstance(frozen, Mapping)
    return frozen


def _placement(value: object, name: str) -> PurePosixPath:
    placement = parse_placement_text(value, name)
    # Listings tell job directories from placement directories by "~", so a placement may not use it.
    if placement_text(placement) != value or "~" in placement_text(placement):
        raise FormatError(f"{name} is not canonical: {value!r}")
    return placement


def _parent(value: object) -> Mapping[str, Frozen] | None:
    if value is None:
        return None
    parent = _exact(value, "parent", _PARENT_MEMBERS)
    for member in ("workspace_id", "job_id", "activation_id", "spawn_id"):
        canonical_uuid(parent[member], f"parent.{member}")
    job_key = require_string(parent["job_key"], "parent.job_key")
    if parse_job_key(job_key)[1] != parent["job_id"]:
        raise FormatError("parent.job_key does not carry the parent.job_id")
    _placement(parent["placement"], "parent.placement")
    return _frozen(parent)


@dataclass(frozen=True)
class JobDefinition:
    """The immutable declaration of one job, as stored in ``job.json`` version 3.

    :param id: The job UUID.
    :param tag: The job tag, or ``None``.
    :param name: The display name.
    :param placement: The authoritative placement below each state directory.
    :param workflow_id: The installed workflow's id.
    :param workflow_name: The workflow's name.
    :param initial_step: The step of the first activation.
    :param priority: The submitted priority, ``0``-``999``.
    :param claim_pool: The claim pool.
    :param required_capabilities: The manager capabilities a claimant must have.
    :param retry_policy: The attempt budgets.
    :param resources: The resource requirements.
    :param step_resources: Per-step resource requirements.
    :param parameters: Opaque job parameters.
    :param declarations: Workflow declarations, keyed by name.
    :param declared: Declared parameter and input metadata, keyed by section.
    :param environment: The declared and overridden workflow environment.
    :param parent: ``{workspace_id, job_id, job_key, placement, activation_id, spawn_id}`` of the spawning job.
    :param seal_succeeded: Whether a succeeded job is sealed; ``None`` uses the workspace setting ``seal.succeeded``.
    :param stored_digest: The SHA-256 of the stored bytes this definition was read from, if any.
    """

    id: str
    tag: str | None
    name: str
    placement: PurePosixPath
    workflow_id: str
    workflow_name: str
    initial_step: str
    priority: int
    claim_pool: str
    required_capabilities: frozenset[str]
    retry_policy: RetryPolicy
    resources: Mapping[str, Frozen]
    step_resources: Mapping[str, Frozen]
    parameters: Mapping[str, Frozen]
    declarations: Mapping[str, Frozen]
    declared: Mapping[str, Frozen]
    environment: Mapping[str, Frozen]
    parent: Mapping[str, Frozen] | None
    seal_succeeded: bool | None
    stored_digest: str | None = field(default=None, compare=False, repr=False)

    @property
    def job_key(self) -> str:
        """The job key, ``[<tag>--]<uuid>``."""

        return make_job_key(self.id, self.tag)

    @property
    def digest(self) -> str:
        """The SHA-256 job digest of the stored bytes, or of :meth:`encode` for an unstored definition."""

        return self.stored_digest if self.stored_digest is not None else job_digest(self.encode())

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> Self:
        """Validate a decoded ``job.json`` strictly: every member present, nothing unknown.

        :param value: The decoded document.
        :return: The definition.
        :raises httk.workflow.errors.FormatError: If the document is malformed.
        """

        doc = _exact(value, "job.json", _MEMBERS)
        if doc["format"] != JOB_FORMAT or doc["format_version"] != JOB_FORMAT_VERSION:
            raise FormatError("job.json is not httk-workflow-job version 3")
        workflow = _exact(doc["workflow"], "workflow", frozenset({"id", "name"}))
        claim = _exact(doc["claim"], "claim", frozenset({"pool", "required_capabilities"}))
        capabilities = claim["required_capabilities"]
        if not isinstance(capabilities, Sequence) or isinstance(capabilities, (str, bytes)):
            raise FormatError("claim.required_capabilities must be an array")
        retry = _exact(doc["retry_policy"], "retry_policy", _RETRY_MEMBERS, optional=_RETRY_MEMBERS)
        seal_succeeded = doc["seal_succeeded"]
        if seal_succeeded is not None and not isinstance(seal_succeeded, bool):
            raise FormatError("seal_succeeded must be a boolean or null")
        placement = _placement(doc["placement"], "placement")
        check_job_placement(placement)
        tag = doc["tag"]
        return cls(
            id=canonical_uuid(doc["id"], "id"),
            tag=None if tag is None else validate_label(tag, "tag"),
            name=require_string(doc["name"], "name"),
            placement=placement,
            workflow_id=require_string(workflow["id"], "workflow.id"),
            workflow_name=require_string(workflow["name"], "workflow.name"),
            initial_step=validate_step(doc["initial_step"], "initial_step"),
            priority=require_int(doc["priority"], "priority", maximum=999),
            claim_pool=validate_label(claim["pool"], "claim.pool"),
            required_capabilities=frozenset(validate_label(item, "capability") for item in capabilities),
            retry_policy=RetryPolicy.from_mapping(retry),
            resources=_frozen(validate_resources(doc["resources"])),
            step_resources=_frozen(_validate_step_resources(doc["step_resources"])),
            parameters=_frozen(validate_parameters(doc["parameters"])),
            declarations=_frozen(validate_declarations(doc["declarations"])),
            declared=_frozen(validate_declared(doc["declared"])),
            environment=_frozen(validate_environment(doc["environment"])),
            parent=_parent(doc["parent"]),
            seal_succeeded=seal_succeeded,
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> Self:
        """Parse stored ``job.json`` bytes, pinning the job digest to them.

        :param data: The stored bytes.
        :return: The definition.
        :raises httk.workflow.errors.FormatError: If the bytes are not a valid document or too large.
        """

        if len(data) > MAX_JOB_BYTES:
            raise FormatError(f"job.json is larger than {MAX_JOB_BYTES} bytes")
        try:
            value = json.loads(data.decode("utf-8"))
        except (ValueError, RecursionError) as exc:
            # ValueError covers undecodable bytes, malformed JSON and over-long integers; deep nesting recurses.
            raise FormatError(f"job.json is not JSON: {exc}") from exc
        if not isinstance(value, Mapping):
            raise FormatError("job.json must be an object")
        return dataclasses.replace(cls.from_mapping(value), stored_digest=job_digest(data))

    @classmethod
    def from_path(cls, path: Path) -> Self:
        """Read one stored ``job.json``: bounded, regular file only, no symlink, never blocking.

        :param path: The file.
        :return: The definition, its digest pinned to the bytes read.
        :raises httk.workflow.errors.FormatError: If the file is missing, unsafe, unreadable or invalid.
        """

        try:
            data = _fs.read_bounded(_fs.loc(path), MAX_JOB_BYTES, nonblock=True)
        except (_fs.UnsafePath, OSError) as exc:
            raise FormatError(f"cannot read {path}: {exc}") from exc
        if data is None:
            raise FormatError(f"{path} does not exist")
        return cls.from_bytes(data)

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON-ready document, with ``format`` and ``format_version``.

        :return: A fresh mutable mapping.
        """

        policy = self.retry_policy
        limits = {
            "maximum_attempts_per_activation": policy.maximum_attempts_per_activation,
            "maximum_total_attempts": policy.maximum_total_attempts,
            "maximum_activations": policy.maximum_activations,
        }
        return {
            "format": JOB_FORMAT,
            "format_version": JOB_FORMAT_VERSION,
            "id": self.id,
            "tag": self.tag,
            "name": self.name,
            "placement": placement_text(self.placement),
            "workflow": {"id": self.workflow_id, "name": self.workflow_name},
            "initial_step": self.initial_step,
            "priority": self.priority,
            "claim": {"pool": self.claim_pool, "required_capabilities": sorted(self.required_capabilities)},
            "retry_policy": {
                **{name: limit for name, limit in limits.items() if limit is not None},
                "retry_on": sorted(policy.retry_on),
            },
            "resources": _thaw(self.resources),
            "step_resources": _thaw(self.step_resources),
            "parameters": _thaw(self.parameters),
            "declarations": _thaw(self.declarations),
            "declared": _thaw(self.declared),
            "environment": _thaw(self.environment),
            "parent": _thaw(self.parent),
            "seal_succeeded": self.seal_succeeded,
        }

    def encode(self) -> bytes:
        """Encode the document as ``job.json`` is written: compact sorted UTF-8 JSON and a newline.

        :return: The bytes.
        :raises httk.workflow.errors.FormatError: If the encoding exceeds :data:`~httk.workflow._job.MAX_JOB_BYTES`.
        """

        data = json_bytes(self.as_mapping()) + b"\n"
        if len(data) > MAX_JOB_BYTES:
            raise FormatError(f"job.json would be larger than {MAX_JOB_BYTES} bytes")
        return data
