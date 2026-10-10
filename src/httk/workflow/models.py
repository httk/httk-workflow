"""Protocol models and validation."""

import hashlib
import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from ._durations import TIME_RESOURCES, parse_slurm_duration
from ._util import (
    DEFAULT_VISIBILITY_DEADLINE_SECONDS,
    json_bytes,
    require_int,
    require_mapping,
    require_number,
    require_string,
)
from .errors import FormatError

CORE_PROFILE = "core-v3"
#: The workspace extension that serves ``WORKSPACE/exchange``: a client-written
#: inbox of ejected bundles and an outbox of returned ones.
EXCHANGE_EXTENSION = "exchange"
SUPPORTED_EXTENSIONS: frozenset[str] = frozenset({EXCHANGE_EXTENSION})
RESERVED_WORKFLOW_ENVIRONMENT_PREFIX = "HTTK_WORKFLOW_"

# Payload entries that belong to a runner rather than to the immutable job: the
# control directory of one attempt, the future run-log directory, and the state
# one runner keeps across the attempts and steps of a job. All live inside the
# payload because they must travel with it, and none is part of what the payload
# digest pins.
ATTEMPTS_DIRECTORY = "attempts"
LOGS_DIRECTORY = "logs"
JOBS_DIRECTORY = "jobs"
POSTPROCESS_DIRECTORY = "postprocess"
EXCHANGE_DIRECTORY = "exchange"
JOB_STATE_DIRECTORY = ".httk-job"
# The envelope a detached or ejected job carries while it is out of every
# workspace: its manifest, its marker, and any shared runner it pins. It
# describes the bundle rather than the job, so no payload digest or seal covers it.
TRANSFER_DIRECTORY = ".httk-transfer"
WORKSPACE_DIRECTORY = ".httk-workspace"
# Workspace policy: the tunables the specification calls "configured", stored
# once in format.json so that two implementations attaching the same workspace
# cannot disagree about them.
POLICY_KEYS = frozenset({"visibility_deadline_seconds", "retention"})
RETENTION_KEYS = frozenset({"attempt_control_days", "trash_days", "owner_tombstone_days"})
#: Members older workspaces still store; reading ignores them and the next policy write drops them.
_RETIRED_POLICY_KEYS = frozenset({"journal_segment_bytes", "lease_seconds"})
_RETIRED_RETENTION_KEYS = frozenset({"journal_days"})
# A deadline longer than a day is a hang rather than a filesystem waiting to settle.
MAXIMUM_VISIBILITY_DEADLINE_SECONDS = 86400.0

# The serialized budget of the optional application-defined ``parameters`` object.
# Parameters describe one job; bulk data belongs in the payload or in transactional
# data, so a small bound keeps job.json readable and cheap to digest.
MAXIMUM_PARAMETERS_BYTES = 262144
# The serialized budget of the optional ``declarations`` object, which gets its
# own allowance of exactly the same size as ``parameters`` for exactly the same
# reason: a declaration describes one job, it is not a place for bulk content.
MAXIMUM_DECLARATIONS_BYTES = 262144
MAXIMUM_ENVIRONMENT_BYTES = 262144
# The serialized budget of the optional ``declared`` object, which mirrors the
# environment member: it carries the declared parameter and input metadata a
# workflow announced, so a later precheck can consume it. Same allowance, same
# reason — it describes one job rather than carrying bulk content.
MAXIMUM_DECLARED_BYTES = 262144

_UUID_PATTERN = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_TAG_PATTERN = re.compile(r"[a-z0-9][a-z0-9._-]{0,47}")
_LABEL_PATTERN = _TAG_PATTERN
# A declaration name is also one file basename below ``.httk-job/declarations/``,
# so it stays within the same conservative character set as every other name the
# protocol coins, plus the underscore the property vocabularies use.
_DECLARATION_NAME_PATTERN = re.compile(r"[A-Za-z0-9_][A-Za-z0-9._-]{0,63}")
_FAILURE_MEMBERS = frozenset({"code", "message", "details", "retryable"})
_UNSAFE_PATH_COMPONENTS = frozenset({"", ".", "..", WORKSPACE_DIRECTORY})


def canonical_uuid(value: object, name: str = "id") -> str:
    """Validate and return a canonical lowercase UUID.

    :param value: The value to validate.
    :param name: The field name used in validation errors.
    :return: The canonical UUID text.
    :raises httk.workflow.errors.FormatError: If the value is not canonical UUID text.
    """
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
    """Validate and return one protocol label.

    :param value: The label to validate.
    :param name: The field name used in validation errors.
    :return: The validated label.
    :raises httk.workflow.errors.FormatError: If the value is not valid label text.
    """
    text = require_string(value, name)
    if not _LABEL_PATTERN.fullmatch(text) or "--" in text:
        raise FormatError(f"{name} has invalid component syntax")
    return text


def is_payload_private(name: str) -> bool:
    """Report whether one payload entry name is runner-private scratch.

    A runner-private entry is excluded from every payload digest, so publishing
    an outcome or writing job state can never change the digest of a payload
    that a manager, a transfer, or a registration check must still recognize.

    :param name: The payload entry name to classify.
    :return: Whether the name is runner-private.
    """

    return name in {ATTEMPTS_DIRECTORY, LOGS_DIRECTORY, JOB_STATE_DIRECTORY}


def validate_parameters(value: object, name: str = "parameters") -> dict[str, object]:
    """Validate the optional application-defined ``parameters`` object of a job.

    The member is opaque to the protocol: only its shape, its key syntax, and
    its serialized size are checked. Its bytes are part of ``job.json`` and are
    therefore covered by the immutable job digest like every other member.

    :param value: The parameters mapping to validate.
    :param name: The field name used in validation errors.
    :return: The validated parameters mapping.
    :raises httk.workflow.errors.FormatError: If the mapping or its serialized contents are invalid.
    """

    mapping = require_mapping(value, name)
    for key in mapping:
        if not isinstance(key, str) or not key:
            raise FormatError(f"{name} keys must be nonempty strings")
    try:
        size = len(json_bytes(mapping))
    except (TypeError, ValueError) as exc:
        raise FormatError(f"{name} must contain only JSON values: {exc}") from exc
    if size > MAXIMUM_PARAMETERS_BYTES:
        raise FormatError(
            f"{name} serializes to {size} bytes, which exceeds the {MAXIMUM_PARAMETERS_BYTES}-byte limit; "
            "put bulk content in the job payload or in transactional data instead"
        )
    return dict(mapping)


def validate_resources(value: object, name: str = "resources") -> dict[str, int]:
    """Validate one quantitative resource-requirement mapping in protocol form.

    Resource names are protocol labels and values are opaque non-negative
    integers, except the reserved time labels ``maxtime`` and ``mintime``,
    which are seconds: ``maxtime`` is at least 1 and ``mintime`` is at most
    ``maxtime`` when one mapping holds both.  The protocol assigns no units or
    meanings to other names.

    :param value: The resource mapping to validate.
    :param name: The field name used in validation errors.
    :return: The validated resource mapping.
    :raises httk.workflow.errors.FormatError: If the mapping, names, or values are invalid.
    """

    mapping = require_mapping(value, name)
    result: dict[str, int] = {}
    for key, raw in mapping.items():
        label = validate_label(key, f"{name} key")
        if isinstance(raw, bool) or not isinstance(raw, int):
            raise FormatError(f"{name}.{label} must be an integer")
        if raw < 0:
            raise FormatError(f"{name}.{label} must be non-negative")
        result[label] = raw
    if result.get("maxtime") == 0:
        raise FormatError(f"{name}.maxtime must be at least 1 second")
    if "maxtime" in result and result.get("mintime", 0) > result["maxtime"]:
        raise FormatError(f"{name}.mintime must not exceed {name}.maxtime")
    return result


def normalize_resources(value: object, name: str = "resources") -> dict[str, int]:
    """Convert one authored resource mapping to protocol form.

    The time labels ``maxtime`` and ``mintime`` must be Slurm ``--time``
    strings and become seconds; other values pass through unchanged.

    :param value: The authored resource mapping.
    :param name: The field name used in validation errors.
    :return: The validated resource mapping in protocol form.
    :raises httk.workflow.errors.FormatError: If the mapping, names, or values are invalid.
    """

    mapping = require_mapping(value, name)
    result: dict[str, object] = {}
    for key, raw in mapping.items():
        if key in TIME_RESOURCES:
            if not isinstance(raw, str):
                raise FormatError(f"{name}.{key} must be a Slurm duration such as 01:30:00")
            try:
                raw = parse_slurm_duration(raw)
            except ValueError as exc:
                raise FormatError(f"{name}.{key}: {exc}") from exc
        result[key] = raw
    return validate_resources(result, name)


def validate_capacity(value: object, name: str) -> dict[str, int]:
    """Validate one manager resource-capacity mapping.

    :param value: The capacity mapping to validate.
    :param name: The field name used in validation errors.
    :return: The validated capacity mapping.
    :raises httk.workflow.errors.FormatError: If the mapping is invalid or names a time label.
    """

    result = validate_resources(value, name)
    reserved = sorted(TIME_RESOURCES & result.keys())
    if reserved:
        raise FormatError(f"{name}.{reserved[0]} is a job requirement, not a manager capacity")
    return result


def _validate_step_resources(value: object, name: str = "step_resources") -> dict[str, dict[str, int]]:
    mapping = require_mapping(value, name)
    result: dict[str, dict[str, int]] = {}
    for raw_step, raw_resources in mapping.items():
        step = validate_step(raw_step, f"{name} step")
        result[step] = validate_resources(raw_resources, f"{name}.{step}")
    return result


def _matches_environment_type(value: object, environment_type: str) -> bool:
    if environment_type == "string":
        return isinstance(value, str)
    if environment_type == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if environment_type == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if environment_type == "boolean":
        return isinstance(value, bool)
    if environment_type == "array":
        return isinstance(value, list)
    return isinstance(value, dict)


def environment_variable_name(setting: str) -> str:
    """Return the environment variable derived from one workflow setting."""

    return "HTTK_" + setting.upper().replace(".", "_")


def _validate_environment_setting(setting: str, name: str) -> None:
    variable = environment_variable_name(setting)
    if variable.startswith(RESERVED_WORKFLOW_ENVIRONMENT_PREFIX):
        raise FormatError(
            f"{name} derives reserved {RESERVED_WORKFLOW_ENVIRONMENT_PREFIX!r} variable {variable!r}; "
            "choose a workflow setting outside the manager-owned namespace"
        )


def validate_environment(value: object, name: str = "environment") -> dict[str, object]:
    """Validate the declared and overridden workflow environment of a job.

    :param value: The environment mapping to validate.
    :param name: The field name used in validation errors.
    :return: The validated environment mapping.
    :raises httk.workflow.errors.FormatError: If the mapping or its values are invalid.
    """

    mapping = require_mapping(value, name)
    declared_raw = require_mapping(mapping.get("declared", {}), f"{name}.declared")
    overrides_raw = require_mapping(mapping.get("overrides", {}), f"{name}.overrides")
    declared: dict[str, dict[str, object]] = {}
    for key, raw in declared_raw.items():
        if not isinstance(key, str) or not key:
            raise FormatError(f"{name}.declared keys must be nonempty strings")
        metadata = require_mapping(raw, f"{name}.declared.{key}")
        unknown = sorted(set(metadata) - {"type", "description", "default", "setting"})
        if unknown:
            raise FormatError(f"{name}.declared.{key} has unsupported members: {', '.join(unknown)}")
        if "description" in metadata:
            require_string(metadata["description"], f"{name}.declared.{key}.description")
        environment_type = metadata.get("type")
        if environment_type is not None:
            environment_type = require_string(environment_type, f"{name}.declared.{key}.type")
            if environment_type not in {"string", "number", "integer", "boolean", "array", "object"}:
                raise FormatError(f"{name}.declared.{key}.type is invalid")
            if "default" in metadata and not _matches_environment_type(metadata["default"], environment_type):
                raise FormatError(f"{name}.declared.{key}.default does not match type {environment_type!r}")
        setting = metadata.get("setting")
        if setting is not None:
            setting = require_string(setting, f"{name}.declared.{key}.setting")
        if (
            setting is not None
            and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*", setting) is None
        ):
            raise FormatError(f"{name}.declared.{key}.setting must be a nonempty dotted identifier")
        effective_setting = setting if setting is not None else key
        _validate_environment_setting(effective_setting, f"{name}.declared.{key}")
        declared[key] = dict(metadata)
    overrides: dict[str, object] = {}
    for key, override in overrides_raw.items():
        if key not in declared:
            raise FormatError(f"{name}.overrides.{key} is not declared")
        environment_type = declared[key].get("type")
        if isinstance(environment_type, str) and not _matches_environment_type(override, environment_type):
            raise FormatError(f"{name}.overrides.{key} does not match type {environment_type!r}")
        overrides[key] = override
    result = {"declared": declared, "overrides": overrides}
    try:
        size = len(json_bytes(result))
    except (TypeError, ValueError) as exc:
        raise FormatError(f"{name} must contain only JSON values: {exc}") from exc
    if size > MAXIMUM_ENVIRONMENT_BYTES:
        raise FormatError(f"{name} exceeds the {MAXIMUM_ENVIRONMENT_BYTES}-byte limit")
    return result


def validate_declared(value: object, name: str = "declared") -> dict[str, dict[str, dict[str, object]]]:
    """Validate the optional ``declared`` object of a job.

    The member mirrors the environment member's shape: it carries the parameter
    and input metadata a workflow announced at submission — under the optional
    ``parameters`` and ``inputs`` sections — so a later precheck can consume the
    declaration without re-resolving the workflow. The protocol only checks its
    structure, key syntax, and serialized size; the meaning of each metadata
    entry is owned by the scaffold that wrote it.

    :param value: The declared mapping to validate.
    :param name: The field name used in validation errors.
    :return: The validated declared sections keyed by ``parameters``/``inputs``.
    :raises httk.workflow.errors.FormatError: If the mapping, its keys, or its size limit are invalid.
    """

    mapping = require_mapping(value, name)
    unsupported = sorted(set(mapping) - {"parameters", "inputs"})
    if unsupported:
        raise FormatError(f"{name} has unsupported members: {', '.join(unsupported)}")
    result: dict[str, dict[str, dict[str, object]]] = {}
    for section in ("parameters", "inputs"):
        if section not in mapping:
            continue
        raw = require_mapping(mapping[section], f"{name}.{section}")
        entries: dict[str, dict[str, object]] = {}
        for key, metadata in raw.items():
            if not isinstance(key, str) or not key:
                raise FormatError(f"{name}.{section} keys must be nonempty strings")
            entries[key] = dict(require_mapping(metadata, f"{name}.{section}.{key}"))
        result[section] = entries
    try:
        size = len(json_bytes(result))
    except (TypeError, ValueError) as exc:
        raise FormatError(f"{name} must contain only JSON values: {exc}") from exc
    if size > MAXIMUM_DECLARED_BYTES:
        raise FormatError(f"{name} exceeds the {MAXIMUM_DECLARED_BYTES}-byte limit")
    return result


def validate_declaration_name(value: object, name: str = "declaration name") -> str:
    """Validate one declaration name of a job.

    The name keys the ``declarations`` object of ``job.json`` and is also the
    basename of the runtime-refined document below ``.httk-job/declarations/``,
    so it must be a safe single path component and nothing else.

    :param value: The declaration name to validate.
    :param name: The field name used in validation errors.
    :return: The validated declaration name.
    :raises httk.workflow.errors.FormatError: If the value is not a safe declaration name.
    """

    text = require_string(value, name)
    if not _DECLARATION_NAME_PATTERN.fullmatch(text) or ".." in text:
        raise FormatError(f"{name} has invalid component syntax")
    return text


def validate_declarations(value: object, name: str = "declarations") -> dict[str, dict[str, object]]:
    """Validate the optional ``declarations`` object of a job.

    Each member is one workflow-declaration document carried verbatim: the
    protocol checks that a declaration is a JSON object and never looks inside
    it, because what the members mean is owned by the vocabulary the document
    names itself — the OPTIMADE workflow-declaration work is standardizing
    exactly that, and an engine that reinterpreted it would only be able to
    disagree with it. The bytes live in ``job.json`` and are therefore covered
    by the immutable job digest like every other member.

    :param value: The declarations mapping to validate.
    :param name: The field name used in validation errors.
    :return: The validated declaration documents keyed by name.
    :raises httk.workflow.errors.FormatError: If a declaration name, document, or size limit is invalid.
    """

    mapping = require_mapping(value, name)
    result: dict[str, dict[str, object]] = {}
    for key, document in mapping.items():
        declaration = validate_declaration_name(key, f"{name} key")
        result[declaration] = dict(require_mapping(document, f"{name}.{declaration}"))
    try:
        size = len(json_bytes(result))
    except (TypeError, ValueError) as exc:
        raise FormatError(f"{name} must contain only JSON values: {exc}") from exc
    if size > MAXIMUM_DECLARATIONS_BYTES:
        raise FormatError(
            f"{name} serializes to {size} bytes, which exceeds the {MAXIMUM_DECLARATIONS_BYTES}-byte limit; "
            "a declaration describes one job, so bulk content belongs in the payload or in transactional data"
        )
    return result


def validate_calls(value: object, source: str) -> dict[str, str]:
    """Validate a mapping from call alias to workflow reference.

    :param value: The mapping to validate.
    :param source: Name the member in error messages.
    :return: The validated mapping.
    :raises httk.workflow.errors.FormatError: If an alias or reference is invalid.
    """

    from httk.core.git_sources import parse_git_uri

    mapping = require_mapping(value, source)
    calls: dict[str, str] = {}
    for alias, reference in mapping.items():
        label = validate_label(alias, f"{source} alias")
        if not isinstance(reference, str) or not reference or any(character.isspace() for character in reference):
            raise FormatError(f"{source}.{label} must be a workflow name or git URI without whitespace")
        if reference.startswith("git+"):
            try:
                pinned = parse_git_uri(reference).pinned
            except ValueError as exc:
                raise FormatError(f"{source}.{label}: {exc}") from exc
            if not pinned:
                raise FormatError(f"{source}.{label} must pin its git URI to a full commit hash: {reference}")
        elif "/" in reference or reference.startswith("."):
            # A path would resolve against whatever directory a job is created from.
            raise FormatError(f"{source}.{label} must name a workflow or a git URI, not a path")
        calls[label] = reference
    shadowed = sorted(
        set(calls) & set(calls.values()) - {alias for alias, reference in calls.items() if alias == reference}
    )
    if shadowed:
        raise FormatError(f"{source} alias {shadowed[0]!r} is also another call's reference")
    return calls


_RUNNER_COMMAND_ELEMENT = re.compile(
    r"(?P<prefix>(?:-*[A-Za-z0-9_.][A-Za-z0-9_.-]*=)?)\{(?P<name>package|artifacts)\}(?P<path>.*)", re.DOTALL
)


@dataclass(frozen=True)
class RunnerCommandReference:
    """One placeholder reference in a runner command element.

    :param name: The placeholder name, ``package`` or ``artifacts``.
    :param path: The relative POSIX path after the placeholder, or ``None`` for the root itself.
    :param prefix: The ``NAME=`` text before the placeholder, or empty at element start.
    """

    name: str
    path: str | None
    prefix: str


def runner_command_reference(element: str, name: str = "runner.command") -> RunnerCommandReference | None:
    """Return the placeholder reference of one runner command element.

    A placeholder may appear only at the element start or directly after
    ``NAME=``, and the rest of the element is either empty or ``/`` followed by a
    relative POSIX path whose parts are nonempty and not ``.`` or ``..``.

    :param element: The command element.
    :param name: The member name used in validation errors.
    :return: The reference, or ``None`` for an element without placeholders.
    :raises httk.workflow.errors.FormatError: If braces appear outside that form.
    """

    if "{" not in element and "}" not in element:
        return None
    match = _RUNNER_COMMAND_ELEMENT.fullmatch(element)
    if match is None:
        raise FormatError(
            f"{name} element {element!r} must place {{package}} or {{artifacts}} at its start or after NAME="
        )
    path = match["path"]
    if not path:
        return RunnerCommandReference(match["name"], None, match["prefix"])
    parts = path[1:].split("/") if path.startswith("/") else []
    if not parts or any(part in {"", ".", ".."} or "{" in part or "}" in part for part in parts):
        raise FormatError(
            f"{name} element {element!r} must continue its placeholder with /PATH, "
            "a relative path of nonempty parts other than '.' and '..'"
        )
    return RunnerCommandReference(match["name"], "/".join(parts), match["prefix"])


def validate_runner_command(value: object, name: str = "runner.command") -> tuple[str, ...]:
    """Validate the structure of a declared runner command.

    A command is a nonempty argument vector whose elements may reference only the
    ``{package}`` and ``{artifacts}`` placeholders, in the form
    :func:`runner_command_reference` accepts. Its program is a placeholder path
    or a bare name resolved on the attempt ``PATH``, never an absolute or
    relative filesystem path.

    :param value: The command value to validate.
    :param name: The member name used in validation errors.
    :return: The unexpanded command.
    :raises httk.workflow.errors.FormatError: If the command is malformed.
    """

    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)) or not value:
        raise FormatError(f"{name} must be a nonempty array of strings")
    command = tuple(require_string(item, f"{name} element") for item in value)
    for element in command:
        if not element or "\x00" in element:
            raise FormatError(f"{name} elements must be nonempty strings without NUL")
        runner_command_reference(element, name)
    program = command[0]
    reference = runner_command_reference(program, name)
    if (reference is None and ("/" in program or program in {".", ".."})) or (
        reference is not None and (reference.prefix or reference.path is None)
    ):
        raise FormatError(
            f"{name} program {program!r} must be {{package}}/PATH, {{artifacts}}/PATH, or a bare name found on PATH"
        )
    return command


def expand_runner_command(command: Sequence[str], package: Path, artifacts: Path | None) -> tuple[str, ...]:
    """Expand the placeholders of a validated runner command.

    :param command: The unexpanded command.
    :param package: The verified runner tree substituted for ``{package}``.
    :param artifacts: The registered build artifacts substituted for ``{artifacts}``.
    :return: The expanded argument vector.
    :raises ValueError: If the command uses ``{artifacts}`` and no artifacts are given.
    """

    expanded: list[str] = []
    for element in command:
        reference = runner_command_reference(element)
        if reference is None:
            expanded.append(element)
            continue
        if reference.name == "artifacts" and artifacts is None:
            raise ValueError("the runner command uses {artifacts} but no build artifacts are registered")
        root = package if reference.name == "package" else artifacts
        assert root is not None
        target = root if reference.path is None else root.joinpath(*reference.path.split("/"))
        expanded.append(f"{reference.prefix}{target}")
    return tuple(expanded)


def job_digest(data: bytes) -> str:
    """Return the normative immutable job digest of stored ``job.json`` bytes.

    The digest of a job is the SHA-256 over the ``job.json`` file bytes exactly
    as submitted. Nothing rewrites or renormalizes those bytes, so the digest is
    reproducible by any implementation with only a hash utility.

    :param data: The stored ``job.json`` bytes.
    :return: The lowercase SHA-256 digest.
    """

    return hashlib.sha256(data).hexdigest()


def validate_step(value: object, name: str = "step") -> str:
    """Validate and return one workflow step name.

    :param value: The step name to validate.
    :param name: The field name used in validation errors.
    :return: The validated step name.
    :raises httk.workflow.errors.FormatError: If the value is not a valid step name.
    """
    text = require_string(value, name)
    if len(text.encode("utf-8")) > 128 or "/" in text or "\x00" in text:
        raise FormatError(f"{name} is not a valid step name")
    return text


def ensure_step_known(step: str, steps: Sequence[str], subject: str) -> str:
    """Return *step* when *subject* implements it, else raise listing the known set.

    :param step: The requested step name.
    :param steps: The steps *subject* is known to implement.
    :param subject: A phrase naming the subject, used verbatim in the error.
    :return: The validated requested step.
    :raises httk.workflow.errors.FormatError: If *step* is malformed or not in *steps*.
    """
    validate_step(step, "step")
    if step in steps:
        return step
    raise FormatError(f"{subject} does not implement the step {step!r}; its steps: {', '.join(steps) or 'none'}")


def normalize_placement(value: str | PurePosixPath) -> PurePosixPath:
    """Validate and normalize one relative POSIX placement.

    The empty placement (``""`` or ``"."``) is legal and normalizes to ``PurePosixPath()``.

    :param value: The placement to validate.
    :return: The normalized relative placement.
    :raises httk.workflow.errors.FormatError: If the placement is absolute, unsafe, or too long.
    """
    placement = PurePosixPath(value)
    if placement.is_absolute():
        raise FormatError("placement must be a relative POSIX path")
    for part in placement.parts:
        if part in _UNSAFE_PATH_COMPONENTS or "\x00" in part:
            raise FormatError(f"invalid placement component: {part!r}")
        if len(part.encode()) > 255:
            raise FormatError(f"placement component is too long: {part!r}")
    return placement


def payload_relative(placement: PurePosixPath, job_key: str) -> PurePosixPath:
    """Return a job payload's workspace-relative path, ``jobs/<placement>/<job_key>``.

    :param placement: The job's normalized placement.
    :param job_key: The job key.
    :return: The relative payload path.
    """
    return PurePosixPath(JOBS_DIRECTORY, *placement.parts, job_key)


def placement_text(placement: PurePosixPath) -> str:
    """Return the canonical text of a placement: ``""`` for the empty placement, else its POSIX form.

    :param placement: The normalized placement.
    :return: The text written to JSON, frames, manifests, and cursors.
    """
    return placement.as_posix() if placement.parts else ""


def parse_placement_text(value: object, name: str = "placement") -> PurePosixPath:
    """Parse the canonical text of a placement, allowing the empty string.

    :param value: The text to parse.
    :param name: The field name used in error messages.
    :return: The normalized placement.
    :raises httk.workflow.errors.FormatError: If the value is not a string or not a valid placement.
    """
    if not isinstance(value, str):
        raise FormatError(f"{name} must be a string")
    return normalize_placement(value)


def check_job_placement(placement: PurePosixPath) -> None:
    """Refuse a placement that names a job directory, so job directories never nest.

    Every job directory is ``<placement>/<job_key>``. A placement none of whose
    components parses as a job key therefore never places one job inside
    another's directory, without any lock or index. New jobs are held to this
    rule wherever they enter a workspace.

    :param placement: The normalized placement to check.
    :raises httk.workflow.errors.FormatError: If a component of the placement parses as a job key.
    """

    for part in placement.parts:
        try:
            parse_job_key(part)
        except FormatError:
            continue
        raise FormatError(
            f"placement {placement_text(placement)!r} has the component {part!r}, which parses as a job key: "
            "a placement must not name a job directory, so job directories never nest"
        )


def make_job_key(job_id: str, tag: str | None) -> str:
    """Compose the stable job key from an identifier and optional tag.

    :param job_id: The job UUID text.
    :param tag: The optional job tag.
    :return: The job key used by workspace markers.
    """
    return f"{tag}--{job_id}" if tag else job_id


def parse_job_key(value: str) -> tuple[str | None, str]:
    """Split a job key into its optional tag and job identifier.

    :param value: The job key to parse.
    :return: The tag and job identifier.
    :raises httk.workflow.errors.FormatError: If the key does not use the protocol syntax.
    """
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

    ``trash_days`` defaults to one day. An explicit ``null`` or ``"keep"``
    value means that category has no limit; omitted ``attempt_control_days``
    likewise remains unlimited. The collector that acts on these numbers is a
    separate concern; the workspace only carries them so that every
    implementation attaching to it agrees on what may be removed and when.

    :param attempt_control_days: The retention period for attempt controls.
    :param trash_days: The retention period for discarded workspace entries.
    :param owner_tombstone_days: How long the ``dead.json`` tombstone of a recovered owner is kept.
    """

    attempt_control_days: float | None = None
    trash_days: float | None = 1.0
    owner_tombstone_days: float | None = 30.0

    @classmethod
    def from_mapping(cls, value: object, name: str = "policy.retention") -> "RetentionPolicy":
        """Validate one retention policy mapping.

        :param value: The policy mapping to validate.
        :param name: The field name used in validation errors.
        :return: The validated retention policy.
        :raises httk.workflow.errors.FormatError: If the mapping contains unsupported or invalid members.
        """
        mapping = require_mapping(value, name)
        unsupported = sorted(set(mapping) - RETENTION_KEYS - _RETIRED_RETENTION_KEYS)
        if unsupported:
            raise FormatError(f"{name} has unsupported members: {', '.join(unsupported)}")

        defaults = cls()

        def optional_days(key: str) -> float | None:
            if key not in mapping:
                return getattr(defaults, key)
            raw = mapping[key]
            if raw is None or raw == "keep":
                return None
            return require_number(raw, f"{name}.{key}", minimum=0.0)

        return cls(
            attempt_control_days=optional_days("attempt_control_days"),
            trash_days=optional_days("trash_days"),
            owner_tombstone_days=optional_days("owner_tombstone_days"),
        )

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON representation, omitting only unset attempt retention.

        :return: The JSON policy mapping.
        """

        result: dict[str, object] = {}
        for key in sorted(RETENTION_KEYS):
            value = getattr(self, key)
            if value is not None or key in {"trash_days", "owner_tombstone_days"}:
                result[key] = value
        return result


@dataclass(frozen=True)
class WorkspacePolicy:
    """The tunables every implementation attaching to one workspace shares.

    These are workspace properties rather than per-process options: two owners
    on different hosts must agree on how long another host's renames and writes
    may take to become visible. They live in ``format.json`` beside the format
    and profile declarations.

    :param visibility_deadline_seconds: The visibility deadline of another host's writes.
    :param retention: The workspace retention policy.
    """

    visibility_deadline_seconds: float = DEFAULT_VISIBILITY_DEADLINE_SECONDS
    retention: RetentionPolicy = RetentionPolicy()

    @classmethod
    def from_mapping(cls, value: object, name: str = "policy") -> "WorkspacePolicy":
        """Validate one complete policy object, filling in absent members.

        :param value: The policy mapping to validate.
        :param name: The field name used in validation errors.
        :return: The validated retention policy.
        :raises httk.workflow.errors.FormatError: If the mapping contains unsupported or invalid members.
        """

        mapping = require_mapping(value, name)
        unsupported = sorted(set(mapping) - POLICY_KEYS - _RETIRED_POLICY_KEYS)
        if unsupported:
            raise FormatError(
                f"{name} has unsupported members: {', '.join(unsupported)}; "
                f"the supported keys are {', '.join(sorted(POLICY_KEYS))}"
            )
        defaults = cls()
        deadline = mapping.get("visibility_deadline_seconds")
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
            retention=(
                defaults.retention
                if retention is None
                else RetentionPolicy.from_mapping(retention, f"{name}.retention")
            ),
        )

    def as_mapping(self) -> dict[str, object]:
        """Return the complete JSON representation stored in ``format.json``.

        :return: The JSON workspace policy mapping.
        """

        return {
            "visibility_deadline_seconds": self.visibility_deadline_seconds,
            "retention": self.retention.as_mapping(),
        }

    def updated(self, changes: Mapping[str, object], name: str = "policy") -> "WorkspacePolicy":
        """Return this policy with *changes* applied and revalidated.

        :param changes: Policy members to replace.
        :param name: The field name used in validation errors.
        :return: The updated workspace policy.
        :raises httk.workflow.errors.FormatError: If the changes contain unsupported or invalid members.
        """

        unsupported = sorted(set(changes) - POLICY_KEYS)
        retention = changes.get("retention")
        if isinstance(retention, Mapping):
            # Reading tolerates the retired members; writing one is refused.
            unsupported += [f"retention.{key}" for key in sorted(set(retention) & _RETIRED_RETENTION_KEYS)]
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

    :param maximum_attempts_per_activation: The per-activation attempt budget.
    :param maximum_total_attempts: The total attempt budget.
    :param maximum_activations: The activation budget.
    :param retry_on: Failure codes eligible for manager-detected retry.
    """

    maximum_attempts_per_activation: int | None
    maximum_total_attempts: int | None
    maximum_activations: int | None
    retry_on: frozenset[str]

    @classmethod
    def from_mapping(cls, value: object) -> "RetryPolicy":
        """Validate one job retry policy mapping.

        :param value: The retry policy mapping to validate.
        :return: The validated retry policy.
        :raises httk.workflow.errors.FormatError: If the mapping contains invalid retry settings.
        """
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
    policy uses ``retry_on`` for manager-detected failures, while a runner-declared
    ``retryable`` failure is retry-eligible regardless of ``retry_on`` when budget
    remains.

    :param code: The stable machine failure code.
    :param message: The human-readable failure message.
    :param details: Optional structured failure details.
    :param retryable: Whether repeating the attempt could help.
    """

    code: str
    message: str
    details: Mapping[str, object] | None = None
    retryable: bool = False

    def as_mapping(self) -> dict[str, object]:
        """Return the canonical JSON representation of this failure.

        :return: The JSON failure mapping.
        """

        result: dict[str, object] = {"code": self.code, "message": self.message}
        if self.details is not None:
            result["details"] = dict(self.details)
        if self.retryable:
            result["retryable"] = True
        return result


def validate_failure(value: object, name: str = "failure") -> Failure:
    """Validate one published failure object.

    :param value: The failure mapping to validate.
    :param name: The field name used in validation errors.
    :return: The validated failure record.
    :raises httk.workflow.errors.FormatError: If the failure is missing required members or has invalid members.
    """

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
