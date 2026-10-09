"""Consumers that turn work authored elsewhere into ordinary *httk₂* jobs.

Each subpackage is one *consumer* of the common execution API — the ``Attempt``
layer of :mod:`httk.workflow.sdk` and the canonical
:mod:`httk.workflow.protocol` — and nothing more. A consumer may use that API;
the API never learns which consumer uses it. The generic manager, workspace,
and CLI see only ordinary jobs, steps, children, and outcomes: no subpackage
here teaches them a compatibility format.

``cwl``
    Common Workflow Language documents (format ``cwl``,
    :mod:`httk.workflow.compat.cwl`).
``pwd``
    Python Workflow Definition documents (format ``pwd``,
    :mod:`httk.workflow.compat.pwd`).
``jobflow``
    jobflow/atomate2 Makers (format ``jobflow``,
    :mod:`httk.workflow.compat.jobflow`).
``v1``
    httk *v1* task packages, run unchanged on the v2 engine, and finished v1
    task trees (format ``httk-v1``, :mod:`httk.workflow.compat.v1`).

This module is the registry of those runner realizations. A subpackage
self-describes via a module-level ``LANGUAGE``, in its package or in its
``realization`` module, and the ``format`` key of a package's
``[workflow.runner]`` table (or ``--format``) names it.

None of these subpackages imports another, none imports the manager or the
generic CLI internals, and none is imported by the common execution layer; the
common layer uses only this registry, which imports the subpackages lazily.
"""

import hashlib
import importlib
import json
import pkgutil
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from importlib.resources import files
from pathlib import Path
from typing import TYPE_CHECKING, Literal

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
]

type DocumentPolicy = Literal["required", "optional", "forbidden"]


class LanguageOutputsMissingError(ValueError):
    """A job of one of these formats has no readable published outputs document."""


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
        except (OSError, ValueError) as exc:
            # A published-but-unreadable document (truncated JSON, permission
            # loss) degrades this one job through the per-job path rather than
            # aborting the whole sweep.
            raise LanguageOutputsMissingError(
                f"{_identity(record)}: outputs document is missing or unreadable; path {path!s}: {exc}"
            ) from exc
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
    """The named input and output ports of one format document.

    :param inputs: Names of the document's input ports.
    :param outputs: Names of the document's output ports.
    """

    inputs: tuple[str, ...]
    outputs: tuple[str, ...]


@dataclass(frozen=True)
class LanguageRequest:
    """The data supplied when preparing one format workflow.

    :param workflow_id: Identify the workflow being prepared.
    :param directory: Locate the workflow package, when it has one.
    :param document: Locate the source workflow document, when it has one.
    :param runner_options: Supply options for the format runner.
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
    """The files and hooks prepared for one format workflow.

    The runner is the realization's own ``<format>_runner.py``, which the
    manager runs for an installed package whose ``runner.builtin`` names the format.

    :param documents: Text or byte documents to write into the payload.
    :param files: Files to stage into the payload.
    :param parameters: Job parameters produced by preparation.
    :param required_capabilities: Require these manager capabilities.
    :param reserved_parameters: Names reserved for per-job realization output.
    :param warnings: Preserve preparation warnings.
    :param instantiate: Supply the per-job hook called after input staging.
    :param finalize: Transform the per-job ``JobSpec`` immediately before its payload is prepared.
    """

    documents: Mapping[str, str | bytes]
    files: Mapping[str, Path]
    parameters: Mapping[str, object]
    required_capabilities: tuple[str, ...] = ()
    reserved_parameters: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    instantiate: Callable[["InstantiateContext"], object] | None = None
    finalize: Callable[["JobSpec"], "JobSpec"] | None = None


@dataclass(frozen=True)
class WorkflowLanguage:
    """The operations one workflow format's realization exposes to the common layer.

    :param name: Name the format, as ``[workflow.runner] format`` spells it.
    :param steps: Declare the runner's steps.
    :param initial_step: Select the runner's initial step.
    :param matches: Identify documents belonging to the format.
    :param ports: Read the input and output ports of a document.
    :param validate_runner: Validate runner options for a document.
    :param prepare: Prepare a format request for execution.
    :param collect: Convert a completed job record into format outputs.
    :param document_policy: State whether package manifests require, allow, or forbid a source document.
    :param open_ports: Skip manifest port validation when document ports cannot be enumerated statically.
    :param has_default_collector: Provide a default collector path.
    :param allows_modes: Permit manifest data and workdir mode overrides.
    :param environment: Declare realization-provided environment metadata.
    :param required_modules: Name the importable modules a job of this format needs at run time.
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
    required_modules: tuple[str, ...] = ()


def _registrations() -> dict[str, WorkflowLanguage]:
    """Return every valid registration beside this module, keyed by its format name."""

    found: dict[str, WorkflowLanguage] = {}
    for module in pkgutil.iter_modules(__path__):
        try:
            candidate = importlib.import_module(f"{__name__}.{module.name}")
        except ImportError:
            continue
        registration = getattr(candidate, "LANGUAGE", None)
        if registration is None and module.ispkg:
            # A consumer that is more than a realization (v1 also reads finished
            # trees) keeps it in a ``realization`` module its package never imports.
            try:
                registration = getattr(importlib.import_module(f"{candidate.__name__}.realization"), "LANGUAGE", None)
            except ImportError:
                continue
        if isinstance(registration, WorkflowLanguage):
            found[registration.name] = registration
    return found


def available_languages() -> tuple[str, ...]:
    """Return the format names of the valid language registrations in this package."""

    return tuple(sorted(_registrations()))


def language(name: str) -> WorkflowLanguage:
    """Return the registered language whose format is *name*.

    :param name: Name the format, using hyphens or underscores.
    :return: The language registration.
    :raises ValueError: If no valid registration carries that format name.
    """

    registrations = _registrations()
    registration = registrations.get(name.replace("_", "-"))
    if registration is None:
        available = ", ".join(sorted(registrations)) or "none"
        raise ValueError(f"unknown workflow format {name!r}; available formats: {available}")
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
    consumer package, e.g. ``httk.workflow.compat.cwl`` — which is where the
    manager finds the runner of an installed package of that format.

    :param package: The consumer package.
    :param name: The runner file name.
    :return: The runner file.
    """

    return Path(str(files(package).joinpath(name)))
