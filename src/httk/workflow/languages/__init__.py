"""The registry of workflow-language runner realizations.

Language modules live beside this module and self-describe via a module-level
``LANGUAGE``.
"""

import hashlib
import importlib
import json
import pkgutil
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from importlib.resources import files
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Literal

from httk.workflow._util import sha256_file

if TYPE_CHECKING:
    import httk.core

    from httk.workflow.collecting import JobRecord
    from httk.workflow.runtime_builders import JobSpec
    from httk.workflow.scaffold import InstantiateContext

__all__ = [
    "DocumentPolicy",
    "LanguageOutputsMissingError",
    "LanguagePorts",
    "LanguageRequest",
    "LanguageScaffold",
    "WorkflowLanguage",
    "available_languages",
    "language",
    "match_document",
    "runner_path",
    "runner_reference",
]

type DocumentPolicy = Literal["required", "optional", "forbidden"]


class LanguageOutputsMissingError(ValueError):
    """A language job has no readable published outputs document."""


def _identity(record: "JobRecord") -> str:
    return f"{record.workspace_id}:{record.job_id}"


def _parameter(record: "JobRecord", name: str, default: str) -> str:
    parameters = record.job.get("parameters")
    return (
        str(parameters[name]) if isinstance(parameters, Mapping) and isinstance(parameters.get(name), str) else default
    )


def _load_outputs(record: "JobRecord", filename: str, prefix: str) -> Mapping[str, object]:
    workdir = record.workdir
    data = record.data
    workdir_path = None if workdir is None else workdir / filename
    data_path = None if data is None else data / prefix / filename
    for path in (workdir_path, data_path):
        if path is None or not path.is_file():
            continue
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except OSError:
            continue
        if not isinstance(value, Mapping):
            raise ValueError(f"{_identity(record)}: outputs document {path} must be a JSON object")
        return value
    raise LanguageOutputsMissingError(
        f"{_identity(record)}: outputs document is missing; tried workdir path {workdir_path!s} "
        f"and data path {data_path!s}"
    )


def _output_roles(record: "JobRecord", name: str, outputs: Mapping[str, object]) -> dict[str, object]:
    parameters = record.job.get("parameters")
    raw = parameters.get(name) if isinstance(parameters, Mapping) else None
    roles = raw if isinstance(raw, Mapping) else {}
    return {
        str(port): roles.get(port, port) if isinstance(roles.get(port, port), str) else str(port) for port in outputs
    }


_CUSTOM_DEFINITIONS: dict[tuple[str, str], "httk.core.PropertyDefinition"] = {}
_ROLE_NAME = re.compile(r"[^A-Za-z0-9_]+")


def _value_kind(value: object) -> tuple[str, str]:
    if isinstance(value, bool):
        return "boolean", ""
    if isinstance(value, (int, float)):
        return "float", ""
    if isinstance(value, str) or value is None:
        return "string", ""
    if isinstance(value, list):
        if value and all(isinstance(item, str) for item in value):
            return "list of string", ""
        if value and all(isinstance(item, (int, float)) and not isinstance(item, bool) for item in value):
            return "list of float", ""
        if value and all(isinstance(item, bool) for item in value):
            return "list of boolean", ""
        return "list of string", "; unsupported heterogeneous, nested, or empty JSON list"
    return "dict", ""


def _data_record(role: str, value: object) -> "httk.core.DataRecord":
    import httk.core

    kind, limitation = _value_kind(value)
    cache_key = (role, kind)
    definition = _CUSTOM_DEFINITIONS.get(cache_key)
    if definition is None:
        sanitized = _ROLE_NAME.sub("_", role).strip("_") or "value"
        suffix = hashlib.sha256(f"{role}\n{kind}".encode()).hexdigest()[:8]
        name = f"_httk_custom_{sanitized}_{suffix}"
        definition = httk.core.PropertyDefinition.from_simple(
            name,
            description=f"Workflow output {role}{limitation}.",
            fulltype=kind,
        )
        _CUSTOM_DEFINITIONS[cache_key] = definition
    return httk.core.DataRecord.from_value(definition.definition_id, definition.name, value)


@dataclass(frozen=True)
class LanguagePorts:
    """The named input and output ports of one language document.

    :param inputs: Names of the document's input ports.
    :param outputs: Names of the document's output ports.
    """

    inputs: tuple[str, ...]
    outputs: tuple[str, ...]


@dataclass(frozen=True)
class LanguageRequest:
    """The data supplied when preparing one language workflow.

    :param workflow_id: Identify the workflow being prepared.
    :param directory: Locate the workflow package, when it has one.
    :param document: Locate the source workflow document, when it has one.
    :param runner_options: Supply options for the language runner.
    :param inputs: Describe the workflow inputs.
    :param outputs: Describe the requested workflow outputs.
    :param parameters: Describe the declared workflow parameters.
    :param environment: Describe the declared workflow environment.
    :param excluded_members: Package members the realization must not stage.
    """

    workflow_id: str
    directory: Path | None
    document: Path | None
    runner_options: Mapping[str, object]
    inputs: Mapping[str, Mapping[str, object]]
    outputs: Mapping[str, Mapping[str, object]]
    parameters: Mapping[str, Mapping[str, object]] = field(default_factory=dict)
    environment: Mapping[str, Mapping[str, object]] = field(default_factory=dict)
    excluded_members: tuple[str, ...] = ()


@dataclass(frozen=True)
class LanguageScaffold:
    """The files, runner, and hooks prepared for one language workflow.

    :param documents: Text or byte documents to write into the payload.
    :param files: Files to stage into the payload.
    :param parameters: Job parameters produced by preparation.
    :param runner: Runner description for the job, when one is supplied.
    :param runner_executor: Select the runner executor.
    :param payload_runner: Name a runner staged in the payload.
    :param workdir_path: Name the workdir below the job payload.
    :param required_capabilities: Require these manager capabilities.
    :param reserved_parameters: Names reserved for per-job realization output.
    :param warnings: Preserve preparation warnings.
    :param instantiate: Supply the per-job hook called after input staging.
    :param finalize: Transform the per-job ``JobSpec`` immediately before its payload is prepared.
    """

    documents: Mapping[str, str | bytes]
    files: Mapping[str, Path]
    parameters: Mapping[str, object]
    runner: Mapping[str, object] | None
    runner_executor: str = "path"
    payload_runner: str | None = None
    workdir_path: str | None = None
    required_capabilities: tuple[str, ...] = ()
    reserved_parameters: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    instantiate: Callable[["InstantiateContext"], object] | None = None
    finalize: Callable[["JobSpec"], "JobSpec"] | None = None


@dataclass(frozen=True)
class WorkflowLanguage:
    """The operations a workflow language exposes to the common layer.

    :param name: Name the language.
    :param steps: Declare the runner's steps.
    :param initial_step: Select the runner's initial step.
    :param matches: Identify documents belonging to the language.
    :param ports: Read the input and output ports of a document.
    :param validate_runner: Validate runner options for a document.
    :param prepare: Prepare a language request for execution.
    :param collect: Convert a completed job record into language outputs.
    :param document_policy: State whether package manifests require, allow, or forbid a source document.
    :param open_ports: Skip manifest port validation when document ports cannot be enumerated statically.
    :param has_default_collector: Provide a default collector path.
    :param allows_modes: Permit manifest data and workdir mode overrides.
    :param environment: Declare language-provided environment metadata.
    """

    name: str
    steps: tuple[str, ...]
    initial_step: str
    matches: Callable[[Path], bool]
    ports: Callable[[Path], LanguagePorts]
    validate_runner: Callable[[Mapping[str, object], Path], None]
    prepare: Callable[[LanguageRequest], LanguageScaffold]
    collect: Callable[["JobRecord"], Mapping[str, object]]
    document_policy: DocumentPolicy = "required"
    open_ports: bool = False
    has_default_collector: bool = True
    allows_modes: bool = True
    environment: Mapping[str, Mapping[str, object]] = field(default_factory=dict)


def available_languages() -> tuple[str, ...]:
    """Return the names of valid language modules available in this package."""

    names: list[str] = []
    for module in pkgutil.iter_modules(__path__):
        try:
            candidate = importlib.import_module(f"{__name__}.{module.name}")
        except ImportError:
            continue
        if isinstance(getattr(candidate, "LANGUAGE", None), WorkflowLanguage):
            names.append(module.name.replace("_", "-"))
    return tuple(sorted(names))


def language(name: str) -> WorkflowLanguage:
    """Return the registered language named *name*.

    :param name: Name the language, using hyphens or underscores.
    :return: The language registration.
    :raises ValueError: If the module or its ``LANGUAGE`` is invalid.
    """

    available = available_languages()
    try:
        module = importlib.import_module(f"{__name__}.{name.replace('-', '_')}")
    except ImportError as exc:
        raise ValueError(f"unknown workflow language {name!r}; available languages: {available}") from exc
    registration = getattr(module, "LANGUAGE", None)
    if not isinstance(registration, WorkflowLanguage):
        raise ValueError(f"invalid workflow language {name!r}; available languages: {available}")
    return registration


def match_document(path: Path) -> WorkflowLanguage | None:
    """Return the first registered language matching *path*, if any."""

    for name in available_languages():
        candidate = language(name)
        if candidate.matches(path):
            return candidate
    return None


def runner_path(package: str, name: str) -> Path:
    """Return the installed file of one packaged compat runner.

    *package* is the importable package the runner file lives beside — its own
    consumer package, e.g. ``httk.workflow.languages.cwl`` — so a compat engine
    resolves and digest-pins the exact bytes it ships rather than a runner owned
    by some shared module.
    """

    return Path(str(files(package).joinpath(name)))


def runner_reference(package: str, name: str) -> dict[str, object]:
    """Return the ``runner`` member of a ``job.json`` running one packaged runner.

    The reserved ``pkg:`` form names the runner inside its own consumer package,
    and the digest is taken from the installed bytes, which is exactly what the
    manager verifies before it stages and executes them.
    """

    path = runner_path(package, name)
    return {
        "executor": "path",
        "source": "installed",
        "path": f"pkg:{package}/{PurePosixPath(name)}",
        "sha256": sha256_file(path),
        "arguments": [],
    }
